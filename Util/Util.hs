module Util.Util
  where

import Pipes
import System.IO
import Control.Monad (unless)
import Control.Applicative

import qualified Data.ByteString.Internal as BS (c2w,w2c)
import qualified Data.ByteString as BSS (hGetLine)

import qualified Data.ByteString.Lazy as BS

splitCSVWithColSkip f str = f $ splitCSV str 

csvWord = BS.c2w ','

splitCSV::BS.ByteString->[BS.ByteString]
splitCSV str = BS.split csvWord str

splitCSV' "" = []
splitCSV' str = [s1] ++ splitCSV'' s2
  where 
  (s1,s2) = break (\x -> ','==x) str

splitCSV'' "" = []
splitCSV'' str = splitCSV' $ tail str


readFileBatch h count readFunc batchFunc = do
  slist <- lift $ readFileBatch' count h []
  yield  $ batchFunc $ map readFunc slist
  let eof = null slist
  unless eof $  readFileBatch h count readFunc batchFunc

readFileBatch'::Int->Handle->[BS.ByteString]->IO [BS.ByteString]
readFileBatch' i h s 
  | i == 0 = do {return s} 
  -- |  lift (hIsEOF h) = do {return s} 
  | otherwise = do
    eof <- hIsEOF h
    case eof of 
      True -> do {return s }
      False -> do  
        str <- BS.fromStrict <$> BSS.hGetLine h
        let newS = str:s 
        let rStr = readFileBatch' (i-1) h newS
        rStr 


getDiv::Int->Int->Float
getDiv p n = (fromInteger px)/(fromInteger nx)
  where
    px = toInteger p
    nx = toInteger n

fstGroup x y= (fst x)==(fst y)

fstSort x y
  | (fst x) == (fst y) = EQ
  | (fst x) < (fst y)  = LT
  | otherwise = GT

sndGroup x y= (snd x)==(snd y)

sndSort x y
  | (snd x) == (snd y) = EQ
  | (snd x) < (snd y)  = LT
  | otherwise = GT