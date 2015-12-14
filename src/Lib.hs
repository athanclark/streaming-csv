{-# LANGUAGE
    OverloadedStrings
  , BangPatterns
  , DeriveGeneric
  , GeneralizedNewtypeDeriving
  #-}

module Lib where

import Pipes.Csv
import qualified Data.Text           as T
import qualified Data.HashMap.Strict as HM

import           Text.PrettyPrint (Doc, (<+>))
import qualified Text.PrettyPrint as PP
import GHC.Generics -- I would make my own instance, but meh
import Control.DeepSeq


-- * Types

-- | The subject-matter of the application - this is the type that is parsed,
--   where each field may be null.
data Session = Session
  { sessionId  :: Maybe T.Text
  , page       :: Maybe T.Text
  , latency    :: Maybe Int
  , timeOnPage :: Maybe Float
  } deriving (Show, Eq, Ord, Generic)

instance ToRecord        Session
instance FromRecord      Session
instance ToNamedRecord   Session
instance FromNamedRecord Session


-- ** Statistics


data NumStat a = NumStat
  { numMin   :: !a
  , numMax   :: !a
  , numTotal :: !a
  , numCount :: {-# UNPACK #-} !Int
  } deriving (Show, Eq, Ord)


newtype TextDistinctCount = TextDistinctCount
  { getTextDistinctCount :: HM.HashMap T.Text Int
  } deriving (Show, Eq, Generic, Monoid, NFData)


-- | Based on the length of the 'Data.Text.Text' parsed
data TextStat = TextStat
  { textMin   :: {-# UNPACK #-} !Int
  , textMax   :: {-# UNPACK #-} !Int
  , textTotal :: {-# UNPACK #-} !Int
  , textDist  :: !TextDistinctCount
  } deriving (Show, Eq, Generic)

instance NFData TextStat


-- | Parsing can fail for individual columns
data ColStat a = ColStat
  { count     :: {-# UNPACK #-} !Int
  , nullCount :: {-# UNPACK #-} !Int
  , mainStat  :: !(Maybe a)
  } deriving (Show, Eq)

-- | Mimicking the schema for 'Session'
data RowStat = RowStat
  { statSessionId  :: !(ColStat TextStat)
  , statPage       :: !(ColStat TextStat)
  , statLatency    :: !(ColStat (NumStat Int))
  , statTimeOnPage :: !(ColStat (NumStat Float))
  } deriving (Show, Eq)


-- * Incremental Statistics

-- ** Numerical

initNumStat :: Num a => a -> NumStat a
initNumStat x = NumStat x x x 1


-- | Change an existing statistic based on another element to add
addNumStat :: ( Num a
              , Ord a
              ) => a            -- ^ Next element
                -> NumStat a    -- ^ Previous Statistic
                -> NumStat a
addNumStat x (NumStat min' max' total' count') =
  NumStat (min x min')
          (max x max')
          (total' + x)
          (1 + count')

{-# INLINEABLE addNumStat #-}

-- ** Textual

initTextStat :: T.Text -> TextStat
initTextStat t = TextStat len len len $ TextDistinctCount (HM.singleton t 1)
  where
    len = T.length t

addDistinctText :: T.Text -> TextDistinctCount -> TextDistinctCount
addDistinctText t = TextDistinctCount
                  . HM.insertWith (+) t 1
                  . getTextDistinctCount

-- | Given the next element, and the count of all previous elements plus the
--   added one, modfify a statistic to include the new element.
addTextStat :: T.Text
            -> TextStat
            -> TextStat
addTextStat t (TextStat min' max' total' ds) =
  let len = T.length t
  in  TextStat (min len min')
               (max len max')
               (total' + len)
               $! addDistinctText t ds


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
                   Nothing -> Just $  initMain x
                   Just m  -> Just $! addMain x m -- crucial to get strict


-- ** Per-Row

initRowStat :: RowStat
initRowStat = RowStat initColStat initColStat initColStat initColStat

-- | Add a session to the total statistics.
addRowStat :: Session -> RowStat -> RowStat
addRowStat sid (RowStat sid' page' lat' pagetime') =
  RowStat (addColStat sessionId  addTextStat initTextStat sid sid')
          (addColStat page       addTextStat initTextStat sid page')
          (addColStat latency    addNumStat  initNumStat  sid lat')
          (addColStat timeOnPage addNumStat  initNumStat  sid pagetime')


-- * Pretty-Printers

ppNumStatInt :: NumStat Int -> Doc
ppNumStatInt (NumStat min' max' total' count') =
  PP.vcat [ PP.text "min:" <+> PP.int (fromIntegral min')
          , PP.text "max:" <+> PP.int (fromIntegral max')
          , PP.text "avg:" <+> PP.int (round $ fromIntegral total' / fromIntegral count')
          ]

ppNumStatDouble :: NumStat Float -> Doc
ppNumStatDouble (NumStat min' max' total' count') =
  PP.vcat [ PP.text "min:" <+> PP.float min'
          , PP.text "max:" <+> PP.float max'
          , PP.text "avg:" <+> PP.float (total' / fromIntegral count')
          ]

ppTextDistinctCount :: TextDistinctCount -> Doc
ppTextDistinctCount (TextDistinctCount xs) =
  PP.vcat $
    (\(k,v) -> PP.nest 5 $ PP.text (T.unpack k ++ ":") <+> PP.int (fromIntegral v))
      <$> (HM.toList xs)

ppTextStat :: TextStat -> Doc
ppTextStat (TextStat min' max' total' ds) =
  PP.vcat [ PP.text "min:" <+> PP.int (fromIntegral min')
          , PP.text "max:" <+> PP.int (fromIntegral max')
          , PP.text "avg:" <+> PP.int (round $ fromIntegral total' / fromIntegral (sum $ getTextDistinctCount ds))
          , PP.text "distincts:" <+> ppTextDistinctCount ds
          ]

ppColStat :: (a -> Doc) -> ColStat a -> Doc
ppColStat ppMain (ColStat count' nullcount mx) =
  PP.vcat [ PP.nest 15 $ PP.text "count:"      <+> PP.int (fromIntegral count')
          , PP.nest 15 $ PP.text "null count:" <+> PP.int (fromIntegral nullcount)
          , case mx of
              Nothing -> PP.nest 15 $ PP.text "no non-nulls"
              Just x  -> PP.nest 15 $ ppMain x
          ]

ppRowStat :: RowStat -> Doc
ppRowStat (RowStat sid page' lat pagetime) =
  PP.vcat [ PP.text "Session Id:"
          , PP.nest 2 $ ppColStat ppTextStat sid

          , PP.text "Page Id:"
          , PP.nest 2 $ ppColStat ppTextStat page'

          , PP.text "Latency:"
          , PP.nest 2 $ ppColStat ppNumStatInt lat

          , PP.text "Time On Page:"
          , PP.nest 2 $ ppColStat ppNumStatDouble pagetime
          ]
