## Overarching
1. (Date Calculations) We need to be smart about how we calculate observation dates. The dates for the radio observations were in MJD, and only had 4 digits for the date (i.e., rounded to the nearest 10000th). Unfortunately, the current code did not guess the correct fifth digit, so all of the radio observations have dates that are ~ -50000 days past outburst. I can think of two potential solutions, but both require a reliance on other partts.
   1. Use the outburst date as a reference point. Having observations that 50 thousand days before outbursts is a dead give away that you have chosen incorrectly.
   2. Use the observations as a reference point.
   I should also state that this is a pretty extreme edge case, so we could just do a hot fix right now and bump a true fix to the back of the line.
2. (Partially deferred) Bundles aren't being made properly. Bundles missing many pieces, including any and all spectroscopic data.
   1. **Note**: We are deferring fixes to bundling of spectroscopic data; such fixes will almost certainly result in much longer redeploy cycles, and we'd like to knock out as many other issues/tasks before increasin the uptime.
   2. The bundle for V1369 Cen (which has only spectroscopic data, no photometry) just has metadata, references, sources, and a README. The sources file has no sources in it. The bib file appears to be fully populated. The metadata file has a spectra count of 0 (zero), but has a references count of 40. Interestingly, the README specifies 122 spectra.
   3. The bundle for V1324 Sco (which has both spectra and photometry) has a photometry fits table (though I haven't checked if it actually contains the data). The sources file has no sources in it. The bib file is empty. The metadata file has a spectra count of 0 (zero), a photometry count of 733, and a reference count of 17. Again, the REAMDE correctly states that there are two spectra (as well as the correct number of photometry and references).

## Spectra (Plot and Observations Table)
1. We currently have "Scale" as a toggle on the spectra that is used for toggling the dates of the spectra, but that name doesn't convey that information very well. We need to come up with a better name for that toggle.
2. Similarly, we should change the name of the "Obserevations" table. What we really mean is spectroscopic observations, as it doesn't include any information about photometry. (Or maybe we should change it so that it also includes ingested photometry catalogs? I actually like that better.)

## Photometry
1. (Lower priority) It appears that the "Date" option for the x-axis in the photometry plot isn't working. It's currently just displaying the MJD. It is working for the spectra.
