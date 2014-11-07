
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

import System.Mem

import qualified Data.Map as DM


splitLength = 10

workdata = "workdata/"


skipCol ls = tail ls  


splitCSV str = skipCol $ splitCSV' str

splitCSV' "" = []
splitCSV' str = [s1] ++ splitCSV'' s2
	where 
	(s1,s2) = break (\x -> ','==x) str

splitCSV'' "" = []
splitCSV'' str = splitCSV' $ tail str

mySplit' [] = []
mySplit' xs = [x] ++  mySplit' t 
	where
	x = take splitLength xs 
	t = drop splitLength xs                

--mySplit::[a]->[(int,[a])]
mySplit xs = zip [1..] ( mySplit' xs)

-- getBlockCount::Ord a => [[a]] -> [[(a,Int)]]
-- getBlockCount t =   map 
--	(map (\x -> ((head x),length x))) $ 
--	map group $ map sort $ transpose t

getBlockCount t = map getCount $ transpose t
 
getCount xs = DM.fromList $  zipWith (\x y -> (,) x  (length y) ) nlist nIndex
	where 
		nlist = nub xs
		nIndex = map (\x -> elemIndices x xs) nlist

foldData::Ord a=> DM.Map a Int -> DM.Map a Int -> DM.Map a Int 
foldData lxs rxs = DM.unionWith (+) lxs rxs


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

readFileBatch h count = do
	slist <- lift $ readFileBatch' count h []
	yield  $ getBlockCount $ map splitCSV slist
	let eof = null slist
	unless eof $  readFileBatch h count

readFileBatch'::Int->Handle->[String]->IO [String]
readFileBatch' i h s 
	| i == 0 = do {return s} 
	-- |  lift (hIsEOF h) = do {return s} 
	| otherwise = do
		eof <- hIsEOF h
		case eof of 
			True -> do {return s }
			False -> do  
				str <- hGetLine h
				let newS = str:s 
				let rStr = readFileBatch' (i-1) h newS
				rStr 





	-- aList <- [ (do { return await}) | y <- [1..10]  ]
	-- putStrLn $ show aList

--tStep::[[(String,Int)]]->[[(String,Int)]]->[[(String,Int)]]
--tStep n a = DS.zipWith foldDataOne n (myMap a)

tStep n a = zipWith foldData n a

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

--initList::[DM.Map [Char] Int]
emptyMap = DM.empty::(DM.Map [Char] Int)

initList = repeat emptyMap

main = do 
	s<- getArgs
	let num =  (1+) $ (\x -> div x splitLength)  $ (read . head) s 
	--let num = (read . head) s 
	--hStr <- runSafeT $ runEffect $ csvFileHandle >-> P.take 1 >->myConsumer
	--return hStr
	--let p =  csvFileHandle >-> P.drop 1 >-> P.take num  
	-- runSafeT $ runEffect $ 
	--let n = P.fold tStep 0 tDone t
	--t <- runSafeT $ P.fold tStep initList tDone p
	-- mapM (\x -> appendFile "testdata"  (show x)) $ toList t
	--runSafeT $ runEffect $ P.fold tStep 0 tDone $ csvFileHandle
	withFile "data/test_rev2" ReadMode $ \h -> do 
		csvHead <- hGetLine h 
		performGC
		t <- P.fold tStep initList tDone $ readFileBatch h splitLength >-> P.take num
		performGC
		let outList = zip (splitCSV csvHead) $ 
			map (\x ->  sortBy (comparing snd) x) $ map DM.toList t 
		mapM (\x -> appendFile "testdata"  ((show x)++ "\n" ) ) outList
		-- p <- 
		--runEffect $ readFileBatch h >-> P.stdoutLn 
	--	t <- runEffect $ readFileBatch h >-> toString
	--	return t
	