{-# LANGUAGE
    Rank2Types
  #-}

module Main where

import Lib

import Pipes
import Pipes.Csv
import Pipes.Safe (runSafeT)
import qualified Pipes.Prelude    as P
import qualified Pipes.ByteString as PS

import Text.PrettyPrint (render)
import Control.Concurrent.STM
import Control.Monad.Trans



sessions :: ( MonadIO m
            ) => Producer Session m ()
sessions = decode HasHeader PS.stdin >-> P.concat

main :: IO ()
main = do
  count <- newTVarIO initRowStat
  runSafeT $ runEffect $
    sessions >-> P.mapM_ (lift . atomically . modifyTVar' count . addRowStat)
  total <- readTVarIO count
  putStrLn $ render (ppRowStat total)
