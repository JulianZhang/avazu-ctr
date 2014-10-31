
import Data.Ord
import Text.CSV.Lazy.String
import Data.List

                

loadTestData = do
	testFile <- readFile "data/test_rev2"
	let cfile = fromCSVTable $ csvTable $ parseCSV testFile
	let coloum = head cfile
	let body = tail cfile
	let coloumList = map ( reverse . sortBy (comparing snd)) $ map (map (\x -> ((head x),length x))) $  map group $ map sort $ transpose body 
	let output = zip coloum coloumList
	writeFile "testdata"  $ unlines $ (map show output)

--sortBy (comparing  snd) $ map (\x -> ((head x),length x)) $ 
