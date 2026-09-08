# MotherDuck and local collection

The current architecture is documented in [LOCAL_DATA_PIPELINE.md](LOCAL_DATA_PIPELINE.md).
The collector preserves received observations on persistent local disk, computes
studies locally, and uploads verified batches to MotherDuck when usage permits.
The dashboard serves saved local summaries; public requests do not query the
cloud database.

Use `MOTHER_DUCK_API_KEY` and `MOTHERDUCK_DATABASE` for the collector's optional
cloud connection. These credentials are not required by the dashboard.
`TRANSIT_SUMMARY_URL` and a server-only `TRANSIT_SUMMARY_TOKEN` connect the
dashboard to the collector's private summary endpoint.

R2 was used by an older hourly/daily Parquet pipeline. The new collector does not
use R2. Preserve any unique historical objects before retiring the old
consolidation service or removing its credentials. Stopping that old job does
not require deleting the archive bucket.

The earlier row-scan pricing and unlimited-RAM claims in this document were
obsolete. MotherDuck currently includes 10 GB storage and 10 CU-hours/month on
Lite; additional usage can be billed. Consult the
[official billing documentation](https://motherduck.com/docs/about-motherduck/billing/managing-billing/)
and the actual account invoice. The new pipeline defaults cloud writes off until
current usage is verified, with local collection continuing independently.
