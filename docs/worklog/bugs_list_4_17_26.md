
1. When exploding out references list, data in the photometry plot migrates outside of the plot (see, GK Per).
2. When you click to, e.g., sort by references, it only sorts by references on that page (likely an issue with the splash page not being the catalog.)
3. Add discovery date and peak mag into initialize_nova
4. Modify documentation post-`refresh_references` rebuild
5. Add a "Date Completed" to the list of finished items on master-list, for change auditing purposes (e.g., we want to know if there's more docuentation that might need to be updated).
6. Add time stamps to tools that I run (e.g., `sweep.py` ), so I can figure out how much time it has been since the code finished (and if I should be worried that I still don't see any changes!)
7. Figure out where to store discovery dates and peak magnitudes files.
8. Implement fundemental change in logic for refresh references
   1. Add in discovery date check; if pre-1960 (I just pulled that out of my butt), then just primary name in ADS query (no aliases).
   2. If query returns >1000 references, search just by primary name (most famous nova, RS Oph, only has ~850 references)
   3. If you initially can't find any sources, do a full text search
   4. Ultimate fallback is to use sources from Simbad search
9. (After item 3 on this list has been finished) Add in logic to photometric and spectroscopic ingestion pipelines that gives better hints for truncated mjd (e.g., if the nova went off on 27783, and the date in the file says 7890, the missing digit is likely a 2).
