
Overarching/Big
1. It doesn't appear that pre-exisiting data products make it to the new deployment version when you upload new data products. (When I use the term deployment here, I am referring to the new file that gets created in our public bucket.) I had spectra data products already loaded into V1324 Sco; I tried to ingest photometry data and it created a new release; the new release had the photometry, but no spectra.
   1. It actually looks like it completely erased the previous nova information when it ingested the photometry. It's not just the spectra that are missing; it also doesn't have any entries in the observations or references.
   2. I tried to rerun the intialize nova workflow (i.e., I gave batch_ingest just the name V1324 Sco). In a fun twist, this deployment didn't have *any* data, but it did have references.
   3. I just downloaded the most recent bundle directly from S3; it doesn't have any of the data products.
   4. I just checked the photometry data table, and the photometry rows still exist. They didn't get deleted. It's just the publication products that are gone.
2. Sparkline isn't showing up on the catalog page. But it was created and lives in S3 for the deployment where we ingested photometry.
3. The link for the bundles leads to an XML sheet that says:
```
This XML file does not appear to have any style information associated with it. The document tree is shown below.
<Error>
<Code>AccessDenied</Code>
<Message>Access Denied</Message>
</Error>
```

Spectra
1. The catalog has ~120 valid spectra, but the website says there's only 68?
2. The spectra still look flat, and that might be because the separation between points is still constant (i.e., there's no LTTB being applied).
   1. I should also say that I plotted the low-res spectra myself, and there are definitely features to be seen. So I'm not sure whey they aren't showing up on the plot. I think it might have something to do with the y-stretch.
3. The spectral lines that we're overlaying on top of the spectra only show their label if you hover at the very top of them, whereas you should be able to see them if you hover anywhere on the line.


Photometry
1. Radio data points don't show up. It does have a Radio/Sub-mm tab on the photometry plot, there's just nothing on it.
   1. I downloaded the photometry file and extracted the radio items; it was able to read/ingest the MJD dates and the frequency, but for some reason wasn't able to ingest the flux and flux error values.
