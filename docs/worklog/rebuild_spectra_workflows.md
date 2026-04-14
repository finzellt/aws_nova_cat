# Rebuild spectra.py workflow

1. Persist four versions of the spectra
   1. (Original) Fits, full resolution, directly from source
   2. "Cleaned" Full resolution (i.e., edges clipped, zeroes removed, and no non-differentiable points)
   3. Full resolution composite CSV (if no other spectra to composite with, this is CSV of original file.)
   4. Down sampled composite CSV
2. Checks down every time we run sweep:
   1. Does everyone have the four required files? If not, go off and build them.
   2. Check for new composites; if they exist, build them!
   3. Recalcualte the number of unique spectral nights
3. When spectra a first ingested:
   1. Save original FITS file (for bundle)
   2. Clean spectra to get rid of zeros and spikes
   3. (For ticket ingested spectra) Calculate wavelength range and approximate SNR
4. Other things that need to get done
   1. Determine spectral regime and/or slice up log wavelength spectra
   2.
