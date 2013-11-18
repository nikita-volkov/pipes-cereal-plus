module PipesCerealPlus 
  (
    -- ** API for \"pipes\"
    serializingProducer,
    deserializingPipe,

    -- ** Reexports of \"cereal-plus\" types
    Serializable(..),
    Serialize,
    Deserialize,
  )
  where

import PipesCerealPlus.Prelude
import qualified CerealPlus.Serializable as Serializable
import qualified CerealPlus.Serialize as Serialize
import qualified CerealPlus.Deserialize as Deserialize
import qualified Pipes.ByteString


deserializingPipe :: 
  (Monad m, Applicative m, Serializable m a) => 
  Pipe ByteString a (EitherT Text m) ()
deserializingPipe = 
  await >>= processBS
  where
    processBS = liftRunPartial (Deserialize.runPartial Serializable.deserialize)
      where
        liftRunPartial runPartial = 
          lift . lift . runPartial >=> \case
            Deserialize.Fail m bs -> lift $ left $ m
            Deserialize.Partial runPartial' -> await >>= liftRunPartial runPartial'
            Deserialize.Done a bs -> yield a >> processBS bs

serializingProducer :: 
  (Monad m, Applicative m, Serializable m a) => 
  a -> Producer ByteString m ()
serializingProducer a = do
  (r, bs) <- lift $ Serialize.runLazy $ Serializable.serialize a
  Pipes.ByteString.fromLazy bs
  return r
