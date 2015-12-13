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
import Data.IORef


sessions :: ( MonadIO m
            ) => Producer Session m ()
sessions = decode HasHeader PS.stdin >-> P.concat

main :: IO ()
main = do
  count <- newIORef initRowStat
  runSafeT $ runEffect $
    sessions >-> P.mapM_ (lift . modifyIORef' count . addRowStat)
  total <- readIORef count
  putStrLn $ render (ppRowStat total)
