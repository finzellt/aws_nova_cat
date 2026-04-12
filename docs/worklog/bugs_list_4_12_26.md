
1. After recent updates (ADR-035), the clipping of red wavelegnth edges that are far past the median appears to be failing.
2. We need to ensure that there is at least somewhat equitable distribution of vertical spacing for all spectra. I have a situation in the UV where two spectra two early spectra take up ~85% of the entire y-stretch of the figure, and the last three spectra get ~15%. You can see essentially nothing on the top three spectra.
3. From the 4-11 worklog, item # 4: When I click on individual spectra and switch to log scaling, I've seen several instances where the minimum y-values get cut off. Again, another issue with the floor.
4. There should be an option for sqrt(flux) scaling when you've selected an individual spectra.
5. I just want to make sure that my memory is correct here: when you select individual spectra you're being shown a high(er) resolution spectra, as compared to the waterfall, right?
