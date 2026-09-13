import express from 'express';
import { constants, gzip } from 'node:zlib';
import type { StudyFilters, StudyCatalog, RowStudyData, SignalStudyData } from '../../src/transit-study-types';
import type { SummaryStore } from './summary-store';
import { emptyStreetcarData } from '../src/streetcar-query';
import { rowStudyFromCells, signalStudyFromCells } from '../src/transit-study-filter';

const emptyNetwork:StudyCatalog={version:'pending',generated_at:'',schedule_hash:'',paths:[],sites:[],row_sections:[],sources:[],limitations:[]};
const pending={status:'collecting' as const,from:null,to:null,network:emptyNetwork,quality:[],method:'pending',limitations:['The collector has not published this study yet. Missing observations do not mean zero delay.']};
const day=(value:unknown):value is string=>typeof value==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(value)&&Number.isFinite(Date.parse(value))&&new Date(value).toISOString().slice(0,10)===value;
function studyFilters(query:Record<string,unknown>,from:string|null,to:string|null):StudyFilters {
  // Both feeds are available by default. Aggregators retain source-specific
  // estimates; the UI chooses one estimate per result without pooling samples.
  const filters:StudyFilters={mode:'streetcar',hour_from:0,hour_to:23};
  if(query.source!=null){if(query.source!=='sse'&&query.source!=='lepass')throw new Error('Choose one observation source.');filters.source=query.source;}
  if(query.mode!=null){if(query.mode!=='streetcar'&&query.mode!=='bus')throw new Error('Choose streetcar or bus.');filters.mode=query.mode;}
  for(const key of ['route_id','direction_id'] as const) if(query[key]!=null){if(typeof query[key]!=='string'||query[key].length>100)throw new Error('Invalid route or direction.');filters[key]=query[key];}
  if(query.day_type!=null&&query.day_type!=='all'){if(query.day_type!=='weekday'&&query.day_type!=='weekend')throw new Error('Invalid day type.');filters.day_type=query.day_type;}
  for(const key of ['hour_from','hour_to'] as const) if(query[key]!=null){const value=Number(query[key]);if(!Number.isInteger(value)||value<0||value>23)throw new Error('Hours must be between 0 and 23.');filters[key]=value;}
  if(filters.hour_from!>filters.hour_to!)throw new Error('The end hour must follow the start hour.');
  for(const key of ['from','to'] as const) if(query[key]!=null){if(!day(query[key]))throw new Error('Use valid YYYY-MM-DD dates.');filters[key]=query[key];}
  if(to)filters.to??=to;
  if(from&&filters.to)filters.from??=[from,new Date(Date.parse(filters.to)-27*86400000).toISOString().slice(0,10)].sort().at(-1)!;
  if(filters.from&&filters.to&&filters.from>filters.to)throw new Error('The end date must follow the start date.');
  if((from&&filters.from&&filters.from<from)||(to&&filters.to&&filters.to>to))throw new Error('That range is outside the saved summary. Choose dates within the available coverage.');
  return filters;
}
export function createSummaryApp(store:Pick<SummaryStore,'snapshot'|'status'>,staticDirectory?:string){
  const app=express();app.disable('x-powered-by');
  app.use('/api',(_req,res,next)=>{res.set('Cache-Control','no-store');res.set('X-Transit-Summary-Stale',String(store.status.stale));if(store.status.generated_at)res.set('X-Transit-Summary-At',store.status.generated_at);next();});
  // Network geometry is shared across studies and can be several megabytes.
  // Compress negotiated large JSON asynchronously; health/error responses stay small.
  app.use('/api',(req,res,next)=>{
    const json=res.json.bind(res);res.vary('Accept-Encoding');
    res.json=(body:unknown)=>{
      if(!req.get('accept-encoding')||!req.acceptsEncodings('gzip')||res.getHeader('Content-Encoding'))return json(body);
      const text=JSON.stringify(body);
      if(text==null||Buffer.byteLength(text)<2048)return json(body);
      gzip(text,{level:constants.Z_BEST_SPEED},(error,compressed)=>{
        if(res.destroyed||res.writableEnded)return;
        if(error){json(body);return;}
        res.set('Content-Encoding','gzip').type('application/json').send(compressed);
      });
      return res;
    };
    next();
  });
  app.get('/api/health',(_req,res)=>res.json({status:'ok',summary:store.status}));
  app.get('/api/source-quality',(_req,res)=>res.json({...store.snapshot?.source_quality??{status:'collecting',sources:[]},snapshot:store.status}));
  app.get(['/api/row-study','/api/signal-study'],(req,res)=>{
    try {
      const isRow=req.path==='/api/row-study';
      const data=isRow?(store.snapshot?.row_study??{...pending,cells:[],comparisons:[],coverage:{passages:0,classified_passages:0,unknown_passages:0}}):(store.snapshot?.signal_study??{...pending,cells:[],signals:[]});
      const filters=studyFilters(req.query,data.from,data.to);
      const result=isRow?rowStudyFromCells(data as RowStudyData,filters):signalStudyFromCells(data as SignalStudyData,filters);
      // Reuse successful study responses across reloads for at most one minute.
      // Express still validates ETags; changing freshness metadata changes the body.
      res.set('Cache-Control','private, max-age=60').json({...result,available_from:data.from,available_to:data.to,filters,snapshot:store.status});
    }catch(error){res.set('Cache-Control','no-store').status(400).json({error:error instanceof Error?error.message:'Invalid study filters.'});}
  });
  const legacyEndpoints={summary:'summary','segment-types':'segment_types',segments:'segments',routes:'routes',hourly:'hourly',daily:'daily','daily-segments':'daily_segments','daily-routes':'daily_routes'} as const;
  for(const [endpoint,key] of Object.entries(legacyEndpoints))app.get(`/api/${endpoint}`,(req,res)=>{
    const value=store.snapshot?.legacy?.[key];
    if(value==null)return res.status(503).json({error:'This overview summary is still being prepared. Other dashboard views remain available.'});
    if(key==='daily_routes'&&req.query.route)return res.json((value as NonNullable<NonNullable<typeof store.snapshot>['legacy']>['daily_routes'])?.filter(row=>row.route===req.query.route));
    return res.json(value);
  });
  app.get('/api/otp',(_req,res)=>res.json(store.snapshot?.legacy?.otp??{status:'not_ready',days:[]}));
  app.get('/api/streetcars',(req,res)=>{
    const corridor=String(req.query.corridor??'st_charles');
    if(!['st_charles','canal','rampart'].includes(corridor))return res.status(400).json({error:'Unknown streetcar corridor.'});
    const data=store.snapshot?.legacy?.streetcars?.[corridor];if(!data)return res.json(emptyStreetcarData());
    try{const filters=studyFilters(req.query,data.available_from??null,data.available_to??null);
      const inRange=(row:{date:string})=>(!filters.from||row.date>=filters.from)&&(!filters.to||row.date<=filters.to);
      return res.json({...data,selected_from:filters.from??data.selected_from,selected_to:filters.to??data.selected_to,bins:data.bins.filter(inRange),site_bins:data.site_bins.filter(inRange),quality:data.quality.filter(inRange)});
    }catch(error){return res.status(400).json({error:error instanceof Error?error.message:'Invalid dates.'});}
  });
  app.get('/api/streetcar-priority',(req,res)=>{
    const corridor=String(req.query.corridor??'st_charles');
    if(!['st_charles','canal','rampart'].includes(corridor))return res.status(400).json({error:'Unknown streetcar corridor.'});
    const data=store.snapshot?.legacy?.streetcar_priority?.[corridor];
    if(!data)return res.status(503).json({error:'Historical priority scenarios have not been saved yet. The current Signals study remains available.'});
    if((req.query.from&&req.query.from!==data.selected_from)||(req.query.to&&req.query.to!==data.selected_to)||(req.query.day_type&&req.query.day_type!=='all')||(req.query.hour_from&&req.query.hour_from!=='0')||(req.query.hour_to&&req.query.hour_to!=='23'))return res.status(400).json({error:'This historical scenario has a fixed date range and all-day coverage. Use the new Signals view for filtered observations.'});
    return res.json({...data,snapshot_only:true});
  });
  if(staticDirectory){app.use(express.static(staticDirectory));app.get('/{*path}',(req,res)=>{if(req.path.startsWith('/api/'))return res.status(404).json({error:'Unknown API endpoint.'});return res.sendFile('index.html',{root:staticDirectory});});}
  return app;
}
