
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

import System.Mem

import qualified Data.Map as DM
import Util.Util


splitLength = 10


skipCol ls = tail ls  




mySplit' [] = []
mySplit' xs = [x] ++  mySplit' t 
	where
	x = take splitLength xs 
	t = drop splitLength xs                

mySplit xs = zip [1..] ( mySplit' xs)


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
	let readCsv = splitCSVWithColSkip skipCol
	withFile "data/test_rev2" ReadMode $ \h -> do 
		csvHead <- hGetLine h 
		performGC
		t <- P.fold tStep initList tDone $ 
			readFileBatch h splitLength 
				readCsv  getBlockCount 
			>-> P.take num
		performGC
		let outList = zip  (readCsv csvHead) $ 
			map (\x ->  sortBy (comparing snd) x) $ map DM.toList t 
		mapM (\x -> appendFile "testdata"  ((show x)++ "\n" ) ) outList

	