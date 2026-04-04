- Big/Major
1. Wasn't able to identify V1324 Sco and V1324_Sco as being the same nova, but was still able to look it up/resolve it in SIMBAD.

- Spectra visualization
1. Need to use the triangle formula for the spectra. Spectra are flat lines because we aren't resolving the single massive peak/spike.
2. Truncate the x-axis based on the median x-max value. We currently have one spectra that stretches out to 2500 nm that is setting the x-scale and squishing the other spectra. Make this decision before we sample, so we can sample the region that will actually be shown.
3. Make sure no spectra have negative (or zero) values (ideally y-min is given some standard value, like 0.1). It's killing (or preventing entirely) the log scale.
4. Figure out what to do with spectra from the same visit but different arms. Have one spectrum from the UVES instrument, which has a blue and a red arm. So we're seeing two spectra--which could, hypothetically, be joined together--for the same day, being presented as two different spectra.
5. (If possible) For the spectral lines we display, make it so their labels are provided when you hover over them, not at the top. The top labels usually get bunched together and its difficult (if not impossible) to see the actual label.


- Photometry visualization
1. First, are we currently equipped to accept radio data? I tried ingesting some and it didn't seem to work.
2. We should treat any error >1 mag as indicating that this is actually a limit. Sometimes people just aren't great about attaching limit flags on their data.
3. The errors should be included when you hover over the dots.
4. Is there a way we can increase the number of unique colors available?
5. What is determining the order of our list of filters? We currently have "B, H, I, J, K, R, UVM2, V" Which makes no sense.

Other Aspects of the Webpage
1. For the table of spectra, truncate the wavelength range to four digits (max).
2. For the table of spectra and references: if possible, show no more than ~3-4 examples,and allow the table to be expanded.
3. For the references table: move the link from the bibcode to the Author/Year.
4. Label the aliases as being such, and make them italicized (and possibly a little smaller). Also, remember to remove the primary name from the list of aliases.


- Update current architecture document
- SO MANY MORE TEST CASES. Especially for the frontend. But also for the backend, so we can be
assured that it is robust/squeaky clean!
- Some kind of override for Idempotency locks when we're just starting to build the catalog (also,
an hour seems *very* generous.)
- Make sure that, for ticket ingestion, we are checking the DDB first, and we *aren't* firing off
an initialize_nova if we find the nova already in the table.
- Fix the issue with (spectra) data validation being slow.
- Create a little flask app for ingesting ticket data





A smooth pipeline for ingesting new data.
