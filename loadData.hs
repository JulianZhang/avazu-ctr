
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

splitLength = 5000

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


--tStep::[[(String,Int)]]->[[(String,Int)]]->[[(String,Int)]]
tStep n a = DS.zipWith foldDataOne n (myMap a)

tDone n = n



seq1 = DS.fromList [(1,2),(2,3),(3,1),(4,2),(5,1),(6,4)]

myMap str =  DS.fromList  $ map (\x -> (x, 1) )  $ tail $ splitCSV str 

initList = DS.replicate 50 $ DS.singleton ("null",0)

main = do 
	s<- getArgs
	let num = (read . head) s 
	--hStr <- runSafeT $ runEffect $ csvFileHandle >-> P.take 1 >->myConsumer
	let p =  csvFileHandle >-> P.drop 1 >-> P.take num  
	-- runSafeT $ runEffect $ 
	--let n = P.fold tStep 0 tDone t
	t <- runSafeT $ P.fold tStep initList tDone p
	mapM (\x -> appendFile "testdata"  (show x)) $ toList t
	--runSafeT $ runEffect $ P.fold tStep 0 tDone $ csvFileHandle
	