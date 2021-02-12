{-# LANGUAGE CPP, ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase, FlexibleContexts #-}

module Database.Beam.Postgres.Streamly where

-- | More efficient query execution functions for @beam-postgres@. These
-- functions use the @conduit@ package, to execute @beam-postgres@ statements in
-- an arbitrary 'MonadIO'. These functions may be more efficient for streaming
-- operations than 'MonadBeam'.


import           Database.Beam
import           Database.Beam.Postgres.Connection
import           Database.Beam.Postgres.Full
import           Database.Beam.Postgres.Syntax
import           Database.Beam.Postgres.Types

import qualified Database.PostgreSQL.LibPQ as Pg hiding
  (Connection, escapeStringConn, escapeIdentifier, escapeByteaConn, exec)
import qualified Database.PostgreSQL.Simple as Pg
import qualified Database.PostgreSQL.Simple.Internal as Pg (withConnection, Field)
import qualified Database.PostgreSQL.Simple.Types as Pg (Query(..))

import qualified Streamly.Prelude as S
import           Streamly.Internal.Prelude (bracketIO)
import           Streamly
import qualified Streamly.Data.Fold as FL
import qualified Streamly.Data.Unfold as UL


import           Data.Int (Int64)
import           Data.Maybe (fromMaybe)
#if !MIN_VERSION_base(4, 11, 0)
import           Data.Semigroup
#endif

import qualified Control.Monad.Fail as Fail


-- * @SELECT@

-- | Run a PostgreSQL @SELECT@ statement in any 'MonadIO'.
runSelect :: ( IsStream t, MonadAsync m, FromBackendRow Postgres a )
          => Pg.Connection -> SqlSelect Postgres a
          -> t m a
runSelect conn (SqlSelect (PgSelectSyntax syntax)) =
  runQueryReturning conn syntax

-- * @INSERT@

-- | Run a PostgreSQL @INSERT@ statement in any 'MonadIO'. Returns the number of
-- rows affected.
runInsert :: MonadIO m
          => Pg.Connection -> SqlInsert Postgres tbl -> m Int64
runInsert _ SqlInsertNoRows = pure 0
runInsert conn (SqlInsert _ (PgInsertSyntax i)) =
  executeStatement conn i

-- | Run a PostgreSQL @INSERT ... RETURNING ...@ statement in any 'MonadIO' and
-- get a 'C.Source' of the newly inserted rows.
runInsertReturning :: ( IsStream t, MonadAsync m, FromBackendRow Postgres a )
                   => Pg.Connection
                   -> PgInsertReturning a
                   -> t m a
runInsertReturning _ PgInsertReturningEmpty = S.nil
runInsertReturning conn (PgInsertReturning i) =
    runQueryReturning conn i

-- * @UPDATE@

-- | Run a PostgreSQL @UPDATE@ statement in any 'MonadIO'. Returns the number of
-- rows affected.
runUpdate :: MonadIO m
          => Pg.Connection -> SqlUpdate Postgres tbl -> m Int64
runUpdate _ SqlIdentityUpdate = pure 0
runUpdate conn (SqlUpdate _ (PgUpdateSyntax i)) =
    executeStatement conn i

-- | Run a PostgreSQL @UPDATE ... RETURNING ...@ statement in any 'MonadIO' and
-- get a 'C.Source' of the newly updated rows.
runUpdateReturning :: ( IsStream t, MonadAsync m, FromBackendRow Postgres a)
                   => Pg.Connection
                   -> PgUpdateReturning a
                   -> t m a
runUpdateReturning _ PgUpdateReturningEmpty = S.nil
runUpdateReturning conn (PgUpdateReturning u) =
  runQueryReturning conn u

-- * @DELETE@

-- | Run a PostgreSQL @DELETE@ statement in any 'MonadIO'. Returns the number of
-- rows affected.
runDelete :: MonadIO m
          => Pg.Connection -> SqlDelete Postgres tbl
          -> m Int64
runDelete conn (SqlDelete _ (PgDeleteSyntax d)) =
    executeStatement conn d

-- | Run a PostgreSQl @DELETE ... RETURNING ...@ statement in any
-- 'MonadIO' and get a 'C.Source' of the deleted rows.
runDeleteReturning :: ( IsStream t, MonadAsync m, FromBackendRow Postgres a )
                   => Pg.Connection -> PgDeleteReturning a
                   -> t m a
runDeleteReturning conn (PgDeleteReturning d) =
  runQueryReturning conn d

-- * Convenience functions

-- | Run any DML statement. Return the number of rows affected
executeStatement ::  MonadIO m => Pg.Connection -> PgSyntax -> m Int64
executeStatement conn x =
  liftIO $ do
    syntax <- pgRenderSyntax conn x
    Pg.execute_ conn (Pg.Query syntax)


-- bracketIO :: (IsStream t, MonadAsync m, MonadCatch m) => m b -> (b -> m c) -> (b -> t m a) -> t m a

type QueryFold a b = forall m. (MonadAsync m) => FL.Fold m a b



-- | Runs any query that returns a set of values
runQueryReturning
  :: forall t m r. ( IsStream t, MonadAsync m, FromBackendRow Postgres r )
  => Pg.Connection -> PgSyntax
  -> t m r
runQueryReturning conn x = bracketIO acquireConnection gracefulShutdown (streamResults Nothing)      
  where
    acquireConnection :: m ()
    acquireConnection = do
      success <- liftIO $ do
        syntax <- pgRenderSyntax conn x
        Pg.withConnection conn (\conn' -> Pg.sendQuery conn' syntax)
      if success then do
        singleRowModeSet <- liftIO (Pg.withConnection conn Pg.setSingleRowMode)
        if not singleRowModeSet
          then error "Could not enable single row mode"
          else return ()
        else do
        errMsg <- fromMaybe "No libpq error provided" <$> liftIO (Pg.withConnection conn Pg.errorMessage)
        error (show errMsg)
    streamResults :: Maybe [Pg.Field] -> () -> t m r
    streamResults fields _ = do
      nextRow <- liftIO (Pg.withConnection conn Pg.getResult)
      case nextRow of
        Nothing -> S.nil
        Just row ->
          liftIO (Pg.resultStatus row) >>=
          \case
            Pg.SingleTuple ->
              do fields' <- liftIO (maybe (getFields row) pure fields)
                 parsedRow <- liftIO (runPgRowReader conn 0 row fields' fromBackendRow)
                 case parsedRow of
                   Left err -> liftIO (bailEarly row ("Could not read row: " <> show err))
                   Right parsedRow' ->
                     do S.yield parsedRow'
                        streamResults (Just fields') ()
            Pg.TuplesOk -> liftIO (Pg.withConnection conn finishQuery) >> S.nil
            Pg.EmptyQuery -> error "No query"
            Pg.CommandOk -> S.nil
            _ -> do errMsg <- liftIO (Pg.resultErrorMessage row)
                    error ("Postgres error: " <> show errMsg)

    bailEarly row errorString = do
      Pg.unsafeFreeResult row
      Pg.withConnection conn $ cancelQuery
      error errorString

    cancelQuery conn' = do
      cancel <- Pg.getCancel conn'
      case cancel of
        Nothing -> pure ()
        Just cancel' -> do
          res <- Pg.cancel cancel'
          case res of
            Right () -> liftIO (finishQuery conn')
            Left err -> error ("Could not cancel: " <> show err)

    finishQuery conn' = do
      nextRow <- Pg.getResult conn'
      case nextRow of
        Nothing -> pure ()
        Just _ -> finishQuery conn'
        
    gracefulShutdown :: () -> m ()
    gracefulShutdown _ =
      liftIO . Pg.withConnection conn $ \conn' ->
      do sts <- Pg.transactionStatus conn'
         case sts of
           Pg.TransIdle -> pure ()
           Pg.TransInTrans -> pure ()
           Pg.TransInError -> pure ()
           Pg.TransUnknown -> pure ()
           Pg.TransActive -> cancelQuery conn'
