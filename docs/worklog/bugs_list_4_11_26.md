## Overarching

## Spectra
1. When you select an individual spectra and zoom in, and then click the reset axes, the spectra it shows only fills about ~10% of the y-axis. So it's still showwing one spectra, but seems to revert to the scaling of the waterfall plot.
2. If you zoom in on a spectra, toggle the spectral line labels, untoggle the spectra line labels, zoom out (i.e., reset the axes), and then toggle the spectral lines again, it zooms you back to where you were before.
3. Add quality cuts to both the spectra compositing and spectra plotting. E.g., we don't want to use a spectra with an SNR of ~7 in a composite with other spectra that have SNR~>30, and we definitely don't want to plot spectra with an SNR of ~10 all by themselves.
4. We need to manage the floor of the spectra (I.e., set a hard floor). When I plot individual spectra I am seeing situations where large chunks are being cutoff, and not just for the first/bottom spectra.  We desperately need hard controls on this.
5. For spectra with incredibly long wavelength ranges--e.g., 350-2500 nm--we need to add something into the code that will allow us to slice off the NIR portion and plot it in the NIR tab (forthcoming).

## Photometry

## Nova Page
1. Need to put the total number of unique spectral visits in the nova page.
2. Need to add MJD to discovery date.

## Catalog
