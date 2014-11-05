
import Data.Ord
import Text.CSV.Lazy.String
import Data.List
import System.IO
import Data.Function (on)
import System.Environment

import Pipes
import qualified Pipes.Prelude as P
import Pipes.Safe (bracket,SafeT(..), runSafeT)

import qualified Data.Sequence as DS

import Data.Foldable (toList)
import Control.Monad (unless)


splitLength = 2

workdata = "workdata/"

splitCSV "" = []
splitCSV str = [s1] ++ splitCSV' s2
	where 
	(s1,s2) = break (\x -> ','==x) str

splitCSV' "" = []
splitCSV' str = splitCSV $ tail str

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

--foldData::Ord a=> [(a,Int)]->[(a,Int)]->[(a,Int)]
foldData lxs rxs = map combind wlist
	where
		wlist = groupBy ((==) `on` fst) $ sortBy (comparing fst) $ lxs ++ rxs
		combind xs 
		 | 1==(length xs) = head xs
		 | 2 ==(length xs) = (((fst . head) xs ), 
		 	((snd . head) xs)+((snd . last) xs))

foldDataOne lxs rs = addData inList lxs rs
	where 
		inList =  DS.findIndexL (\x -> (fst rs) == (fst x)) lxs

-- addData::[Int]->seq a->seq b ->seq a
addData Nothing lxs rs = lxs DS.|> rs 
addData (Just is) lxs rs = DS.adjust addItem is lxs
	where 
	addItem (s,i) = (s,i+1)

csvFileHandle::Producer' String (SafeT IO) ()
csvFileHandle =  bracket 
	(do {h <- openFile "data/test_rev2" ReadMode;return h}) 
	(\h ->return h)
	P.fromHandle

--csvFileBatchProducer::Producer' [[String]] IO ()
--csvFileBatchProducer= do 
--	withFile "data/sample" ReadMode (\h-> readFileBatch h)

readFileBatch h = do
	slist <- lift $ readFileBatch' h []
	yield  slist
	let eof = null slist
	unless eof $  readFileBatch h

readFileBatch'::Handle->[String]->IO [String]
readFileBatch' h s 
	| (length s)>=splitLength = do {return s} 
	-- |  lift (hIsEOF h) = do {return s} 
	| otherwise = do
		eof <- hIsEOF h
		case eof of 
			False -> do {return s }
			True -> do  
				str <- hGetLine h
				let newS = s ++ [str]
				let rStr = readFileBatch' h newS
				rStr 




loadTestData datalenthg = do
	testFile <- readFile "data/test_rev2"
	let cfile = fromCSVTable $ csvTable $ parseCSV testFile
	let coloum = head cfile
	let body = take datalenthg $ tail cfile
	let countData = foldl1' (zipWith  foldData)  $ map  getBlockCount  $ mySplit' body 
	let output =  zip coloum  $ map ( reverse . sortBy (comparing snd) ) countData
	appendFile "testdata" $ foldl1 (\x y -> x ++"\n"++y)$ map show $tail output

	-- aList <- [ (do { return await}) | y <- [1..10]  ]
	-- putStrLn $ show aList

--tStep::[[(String,Int)]]->[[(String,Int)]]->[[(String,Int)]]
--tStep n a = DS.zipWith foldDataOne n (myMap a)

tStep n a = n ++ a

tDone n = n

myConsumer::Consumer String (SafeT IO) [String]
myConsumer = do
	str <- await
	return $ splitCSV str

seq1 = DS.fromList [(1,2),(2,3),(3,1),(4,2),(5,1),(6,4)]

myMap str =  DS.fromList  $ map (\x -> (x, 1) )  $ tail $ splitCSV str 

toString::Consumer [String] IO String
toString = do
	sl <- await
	return $ show sl

initList = [[]] -- DS.replicate 50 $ DS.singleton ("null",0)

main = do 
	s<- getArgs
	--let num = (read . head) s 
	hStr <- runSafeT $ runEffect $ csvFileHandle >-> P.take 1 >->myConsumer
	return hStr
	--let p =  csvFileHandle >-> P.drop 1 >-> P.take num  
	-- runSafeT $ runEffect $ 
	--let n = P.fold tStep 0 tDone t
	--t <- runSafeT $ P.fold tStep initList tDone p
	-- mapM (\x -> appendFile "testdata"  (show x)) $ toList t
	--runSafeT $ runEffect $ P.fold tStep 0 tDone $ csvFileHandle
	--withFile "data/sample.csv" ReadMode $ \h -> do  
		-- t <- P.fold tStep initList tDone $ readFileBatch h
		-- return t
	--	t <- runEffect $ readFileBatch h >-> toString
	--	return t
	