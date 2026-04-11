
1. Fix the loggers for the Fargate operations so they can actually be properly sorted..
2. Add in interpolation to use all of the spectra from a given night--it currently puts together some wavelength regimes
3. Create a single app for reseeding/redeploying/rebuilding. We have several pieces of code that do this, but it would be nice to have them all in one spot (with a gui!)
4. Change the units of the line features from angstroms to nm; similarly, get rid of the angstrom symbol on the hydrogen features.
5. Make a STIS adapter for spectra
6. Add in "Spectral Visits" as a data column, corrsponding to the number of unique nights.
7. Create our own pipeline for UVES spectra reduction
8. (Big task/low priority) Come up with a plan/design for recurrent novae
