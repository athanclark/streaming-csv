{-# LANGUAGE
    OverloadedStrings
  , BangPatterns
  , DeriveGeneric
  #-}

module Lib where

import Pipes
import Pipes.Csv
import qualified Pipes.Prelude    as P
import qualified Data.Text        as T
import qualified Data.ByteString  as BS

import           Text.PrettyPrint (Doc, (<>), (<+>), ($$))
import qualified Text.PrettyPrint as PP
import GHC.Generics -- I would make my own instance, but meh


-- * Types

-- | The subject-matter of the application - this is the type that is parsed,
--   where each field may be null.
data Session = Session
  { sessionId  :: Maybe T.Text
  , page       :: Maybe T.Text
  , latency    :: Maybe Int
  , timeOnPage :: Maybe Double
  } deriving (Show, Eq, Ord, Generic)

instance ToRecord        Session
instance FromRecord      Session
instance ToNamedRecord   Session
instance FromNamedRecord Session


-- ** Statistics

data NumStat a = NumStat
  { numMin :: a
  , numMax :: a
  , numAvg :: a
  } deriving (Show, Eq, Ord)


-- | Based on the length of the 'Data.Text.Text' parsed
data TextStat = TextStat
  { textMin :: {-# UNPACK #-} !Int
  , textMax :: {-# UNPACK #-} !Int
  , textAvg :: {-# UNPACK #-} !Int
  } deriving (Show, Eq, Ord)


-- | Parsing can fail for individual columns
data ColStat a = ColStat
  { count     :: {-# UNPACK #-} !Int
  , nullCount :: {-# UNPACK #-} !Int
  , mainStat  :: Maybe a
  } deriving (Show, Eq, Ord)

-- | Mimicking the schema for 'Session'
data RowStat = RowStat
  { statSessionId  :: ColStat TextStat
  , statPage       :: ColStat TextStat
  , statLatency    :: ColStat (NumStat Int)
  , statTimeOnPage :: ColStat (NumStat Double)
  } deriving (Show, Eq, Ord)


-- * Incremental Statistics

-- ** Numerical

initNumStat :: Num a => a -> NumStat a
initNumStat x = NumStat x x x

-- | Change an existing statistic based on another element to add
addNumStat :: ( Num a
              , Ord a
              ) => (Double -> a) -- ^ @fromFractional@
                -> (a -> Double) -- ^ @toFractional@
                -> Int           -- ^ /count/
                -> a             -- ^ Next element
                -> NumStat a     -- ^ Previous Statistic
                -> NumStat a
addNumStat fromFractional toFractional count x (NumStat min' max' oldAvg) =
  NumStat (min x min')
          (max x max') $ fromFractional $
            toFractional ((fromIntegral (count-1) * oldAvg) + x)
          / fromIntegral count

-- ** Textual

initTextStat :: T.Text -> TextStat
initTextStat t = TextStat len len len
  where
    len = T.length t

-- | Given the next element, and the count of all previous elements plus the
--   added one, modfify a statistic to include the new element.
addTextStat :: Int -- ^ Inclusive count
            -> T.Text
            -> TextStat
            -> TextStat
addTextStat count t (TextStat min' max' oldAvg) =
  TextStat (min (T.length t) min')
           (max (T.length t) max') $ floor $
          -- solution based on http://math.stackexchange.com/questions/106700/incremental-averageing#answer-106720
             fromIntegral (((count-1) * oldAvg) + T.length t)
           / fromIntegral count


-- ** Per-Column

initColStat :: ColStat a
initColStat = ColStat 0 0 Nothing

-- | Given a way to potentially get a new element, and a way to add the element
--   to the existing contained statistic, add a 'Session' (the main element type)
--   to the column's domain-specific statistic.
addColStat :: (Session -> Maybe x) -- ^ Get the element out of the 'Session'
           -> (x -> a -> a)        -- ^ Add the element to the 'mainStat' statistic of the column
           -> (x -> a)             -- ^ Create a new statistic from a single element
           -> Session              -- ^ The session to add
           -> ColStat a            -- ^ The old statistic for the column
           -> ColStat a
addColStat measure addMain initMain sid (ColStat c nc mMain) =
  case measure sid of
    Nothing -> ColStat c (nc+1) mMain
    Just x  -> ColStat (c+1) nc $
                 case mMain of
                   Nothing -> Just (initMain x)
                   Just m  -> Just (addMain x m)

-- ** Per-Row

initRowStat :: RowStat
initRowStat = RowStat initColStat initColStat initColStat initColStat

-- | Add a session to the total statistics.
addRowStat :: Session -> RowStat -> RowStat
addRowStat sid (RowStat sid' page' lat' pagetime') =
  RowStat (addColStat sessionId  addSid      initTextStat sid sid')
          (addColStat page       addPage     initTextStat sid page')
          (addColStat latency    addLat      initNumStat  sid lat')
          (addColStat timeOnPage addPageTime initNumStat  sid pagetime')
  where
    addSid      = addTextStat (count sid')
    addPage     = addTextStat (count page')
    addLat      = addNumStat floor fromIntegral (count lat')
    addPageTime = addNumStat id    id           (count pagetime')


-- * Pretty-Printers

ppNumStatInt :: NumStat Int -> Doc
ppNumStatInt (NumStat min' max' avg') =
  PP.vcat [ PP.text "min:" <+> PP.int min'
          , PP.text "max:" <+> PP.int max'
          , PP.text "avg:" <+> PP.int avg'
          ]

ppNumStatDouble :: NumStat Double -> Doc
ppNumStatDouble (NumStat min' max' avg') =
  PP.vcat [ PP.text "min:" <+> PP.double min'
          , PP.text "max:" <+> PP.double max'
          , PP.text "avg:" <+> PP.double avg'
          ]

ppTextStat :: TextStat -> Doc
ppTextStat (TextStat min' max' avg') =
  PP.vcat [ PP.text "min:" <+> PP.int min'
          , PP.text "max:" <+> PP.int max'
          , PP.text "avg:" <+> PP.int avg'
          ]

ppColStat :: (a -> Doc) -> ColStat a -> Doc
ppColStat ppMain (ColStat count nullcount mx) =
  PP.vcat [ PP.nest 5 $ PP.text "count:" <+> PP.int count
          , PP.nest 5 $ PP.text "null count:" <+> PP.int nullcount
          , case mx of
              Nothing -> PP.nest 5 $ PP.text "no non-nulls"
              Just x  -> PP.nest 5 $ ppMain x
          ]

ppRowStat :: RowStat -> Doc
ppRowStat (RowStat sid page lat pagetime) =
  PP.vcat [ PP.text "Session Id:"
          , PP.nest 2 $ ppColStat ppTextStat sid

          , PP.text "Page Id:"
          , PP.nest 2 $ ppColStat ppTextStat page

          , PP.text "Latency:"
          , PP.nest 2 $ ppColStat ppNumStatInt lat

          , PP.text "Time On Page:"
          , PP.nest 2 $ ppColStat ppNumStatDouble pagetime
          ]
