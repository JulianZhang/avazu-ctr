ghc -O -prof -fprof-auto -rtsopts loadData.hs

./loadData 120000 +RST -p

../../ws/VisualProf/.cabal-sandbox/bin/visual-prof -px loadData.hs loadData 10000