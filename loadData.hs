
import Data.Ord
import Text.CSV.Lazy.String
import Data.List
import System.IO
import Data.Function (on)
import System.Environment

import Pipes
import qualified Pipes.Prelude as P
import Pipes.Safe (bracket,SafeT(..), runSafeT)

splitLength = 5000

workdata = "workdata/"

mySplit' [] = []
mySplit' xs = [x] ++  mySplit' t 
	where
	x = take splitLength xs 
	t = drop splitLength xs                

--mySplit::[a]->[(int,[a])]
mySplit xs = zip [1..] ( mySplit' xs)

getBlockCount::Ord a => [[a]] -> [[(a,Int)]]
getBlockCount t =   map 
	(map (\x -> ((head x),length x))) $ 
	map group $ map sort $ transpose t

foldData::Ord a=> [(a,Int)]->[(a,Int)]->[(a,Int)]
foldData lxs rxs = map combind wlist
	where
		wlist = groupBy ((==) `on` fst) $ sortBy (comparing fst) $ lxs ++ rxs
		combind xs 
		 | 1==(length xs) = head xs
		 | 2 ==(length xs) = (((fst . head) xs ), ((snd . head) xs)+((snd . last) xs))

csvFileHandle::Producer' String (SafeT IO) ()
csvFileHandle =  bracket 
	(do {h <- openFile "data/sample.csv" ReadMode;return h}) 
	(\h ->return h)
	P.fromHandle  


loadTestData datalenthg = do
	testFile <- readFile "data/test_rev2"
	let cfile = fromCSVTable $ csvTable $ parseCSV testFile
	let coloum = head cfile
	let body = take datalenthg $ tail cfile
	let countData = foldl1' (zipWith  foldData)  $ map  getBlockCount  $ mySplit' body 
	let output =  zip coloum  $ map ( reverse . sortBy (comparing snd) ) countData
	appendFile "testdata" $ foldl1 (\x y -> x ++"\n"++y)$ map show $tail output

myConsumer::Consumer String IO ()
myConsumer = do
	aList <- [ (do { return await}) | y <- [1..10]  ]
	putStrLn $ show aList

main = do 
	s<- getArgs
	let num = (read . head) s 
	runSafeT $ runEffect $ csvFileHandle >-> P.take num >-> P.stdoutLn
	