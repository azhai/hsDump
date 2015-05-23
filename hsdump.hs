{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
-- cabal update && cabal install cabal-install
-- cabal install cassava && cabal install yaml-config && cabal install hdbc-mysql
-- ghc -threaded -O2 -optc-O3 -funfolding-use-threshold=16 -fforce-recomp hsdump.hs

module Main where
import Control.Monad (zipWithM_)
import Data.Char (ord)
import Data.Csv
import Data.Convertible
import qualified Data.ByteString.Lazy as Bytes (appendFile)
import qualified Data.List.Utils as Utils (join)
import qualified Data.Yaml.Config as Yaml
import Database.HDBC
import Database.HDBC.MySQL
import System.Directory (createDirectoryIfMissing)
import System.Environment (getArgs)

{- 连接数据库 -}
connectDB :: Yaml.Config -> IO Connection
connectDB dbconf =
    do
        connectMySQL defaultMySQLConnectInfo {
                mysqlHost       = host,
                mysqlPort       = port,
                mysqlUser       = user,
                mysqlPassword   = password,
                mysqlDatabase   = database,
                mysqlUnixSocket = socket
            }
    where
        host = Yaml.lookupDefault "host" "localhost" dbconf :: String
        port = Yaml.lookupDefault "port" 3306 dbconf :: Int
        user = Yaml.lookupDefault "user" "root" dbconf :: String
        password = Yaml.lookupDefault "password" "" dbconf :: String
        database = Yaml.lookupDefault "database" "test" dbconf :: String
        socket = Yaml.lookupDefault "socket" "/var/lib/mysql.sock" dbconf :: String

{- 拼接SQL语句 -}
concatSql :: Yaml.Config -> String
concatSql sqlconf =
    "SELECT " ++ (Utils.join "," fields) ++ " FROM " ++ table
        ++ " WHERE " ++ index ++ ">=? AND " ++ index ++ "<? ORDER BY " ++ order
    where
        (table:_) = Yaml.lookup "table" sqlconf :: [String]
        (index:_) = Yaml.lookup "index" sqlconf :: [String]
        (order:_) = Yaml.lookup "order" sqlconf :: [String]
        fields = Yaml.lookupDefault "fields" ["*"] sqlconf :: [String]

fromMysql :: SqlValue -> Maybe String
fromMysql SqlNull = Just "\\N"
fromMysql x = fromSql x

{- 输出结果到Tab分隔的CSV文件 -}
dumpRecords :: String -> Connection -> String -> String -> String -> IO ()
dumpRecords outpath conn sql start_day stop_day =
    do
        rows <- withRTSSignalsBlocked $ do
            quickQuery conn sql [toSql start_day, toSql stop_day]
        Bytes.appendFile output $ encodeWith encOpts $
            [ (map fromMysql row::[Maybe String]) | row <- rows ]
    where
        encOpts = defaultEncodeOptions { encDelimiter = fromIntegral (ord '\t') }
        dayname = filter (/='-') start_day  -- 去掉日期中间的横杠
        output = outpath ++ dayname ++ ".txt"

main :: IO ()
main = do
    args <- getArgs             -- 读命令行参数，1个参数：yaml文件名
    conf <- Yaml.load (head args)    -- 加载yaml中的配置
    dbconf <- Yaml.subconfig "db" conf
    conn <- connectDB dbconf
    sqlconf <- Yaml.subconfig "sql" conf
    outconf <- Yaml.subconfig "out" conf
    let sql = concatSql sqlconf
        path = Yaml.lookupDefault "path" "./data" outconf :: String
        pre = Yaml.lookupDefault "pre" "records-" outconf :: String
        outpath = path ++ "/" ++ pre
        (days:_) = Yaml.lookup "days" conf :: [[String]]
    createDirectoryIfMissing True path  -- 如果目录不存在，就创建
    {- 调用查询输出结果命令，以days中相邻两个日期作为最后两个参数 -}
    zipWithM_ (dumpRecords outpath conn sql) days (tail days)
    disconnect conn
