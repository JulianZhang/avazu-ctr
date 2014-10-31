
import Data.Ord
import Text.CSV.Lazy.String
import Data.List

splitLength = 50

workdata = "workdata/"

mySplit' [] = []
mySplit' xs = [x] ++  mySplit' t 
	where
	x = take splitLength xs 
	t = drop splitLength xs                

--mySplit::[a]->[(int,[a])]
mySplit xs = zip [1..] ( mySplit' xs)

getCountData::Ord a => [[a]] -> [[(a,Int)]]
getCountData t =  map ( reverse . sortBy (comparing snd)) $ map 
	(map (\x -> ((head x),length x))) $ 
	map group $ map sort $ transpose t

loadTestData = do
	testFile <- readFile "data/test_rev2"
	let cfile = fromCSVTable $ csvTable $ parseCSV testFile
	let coloum = head cfile
	let body = take 1000 $ tail cfile
	let countData = map (\(x,y) -> (x , getCountData y )) $ mySplit body 
	-- let output = zip coloum coloumList
	mapM (\(_,x) -> appendFile "testdata" ( (show x) ++ "\n") )  countData
	


--sortBy (comparing  snd) $ map (\x -> ((head x),length x)) $ 
