import type { CaptureExchange, CaptureBundleDescriptor } from './capture-exchange';
import type { LocalJournal } from './local-journal';

/** Seal every frame visible at claim time; ongoing capture belongs to the next job.
 * Caller serializes this with its normal packaging timer. */
export async function sealCaptureFrontier(exchange: CaptureExchange, journal: LocalJournal): Promise<CaptureBundleDescriptor|null> {
  const pending=new Set(await journal.files(Number.MAX_SAFE_INTEGER));
  const snapshot=Object.create(journal) as LocalJournal;
  snapshot.files=async(limit=500)=>[...pending].slice(0,limit);
  snapshot.acknowledge=async path=>{await journal.acknowledge(path);pending.delete(path);};
  let latest:CaptureBundleDescriptor|null=null,recoveryPasses=0;
  while(pending.size){
    const before=pending.size;
    latest=await exchange.seal(snapshot);
    if(!latest)throw new Error('Capture frontier did not advance; original frames remain pending');
    // A prior seal may have unlinked its last frame before crashing. Finishing
    // that intent once is valid even though none of this frontier was consumed.
    if(pending.size>=before){if(++recoveryPasses>1)throw new Error('Capture frontier did not advance; original frames remain pending');}
    else recoveryPasses=0;
  }
  return latest;
}
