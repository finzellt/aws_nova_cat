
The Sparkline is working! Hurray! Also, the plots for V1369 Cen are no longer flat!! That's been a thorn in our side for far too long.
I think we can knock off/remove issues B3, B7, and B11 from our master task list. Unfortunately, there doesn't appear to be any change in the dates for the radio. So B10 remains.


## Overarching
1. (Low-priority) The left/right margins for the Sparkline are off. There's a small gap on the left between the image and reference count, and a large(r) gap on the right between the image and the end of the row.


## Spectra
1. **Note:** A core part of my plotting philosophy is that we squeeze as much use out of the y-axis (and x-axis) as humanly possible. Aesthetics is our secondary concern, but there's a pretty big gap between 1st and second priority. We should build tools and tests around this concept.
2. V1369 Cen (which has many spectra):
   1. Issue with the selection of novae to plot. There are two spectra from Day 8, two spectra from Day 14, and two spectra from Day 837. As you might expect, they are (virtually) identical, and aren't providing any new information. We need to ensure that we aren't taking spectra from the same day. In fact, we should only plot the maximum number of spectrum (11, I believe) if there is a really good reason to (I.e., we have very very well sampled spectra and we can cover large intervals in log space); our default should be to try to *not* plot the maximum, and leave more y-stretch for the spectra that we do plot.
3. For V1324 Sco (which only has two spectra)
   1. Spectra cutoff at the top. We aren't ensuring that the peak of the top spectra fits within the default display window.
   2. Spectra not making good use of the space. There's a hard line that says "Day 3" that's about 15% of the way up the plot (meaning that 15% is being wasted). And then there's a decent gap between the two spectra. And then the top spectra is cutoff.
