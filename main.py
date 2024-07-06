import config
from database.database import DBManager
from benchmark.blind.blind import Blind
from benchmark.tpcc.tpcc import TPCC
from benchmark.twitter.twitter import Twitter

if __name__ == "__main__":
    # db = DBManager(config.PostgreSQLConfig)
    # blind_manager = Blind(db, 50, 50)
    # blind_manager.create_tables()
    # blind_manager.load()
    # blind_manager.run()

    # db = DBManager(config.PostgreSQLConfig)
    # blind_manager = Blind(db, 20, 80)
    # blind_manager.create_tables()
    # blind_manager.load()
    # blind_manager.run()
    
    # db = DBManager(config.PostgreSQLConfig)
    # blind_manager = Blind(db, 80, 20)
    # blind_manager.create_tables()
    # blind_manager.load()
    # blind_manager.run()
    
    db = DBManager(config.PostgreSQLConfig)
    twitter_manager = Twitter(db)
    twitter_manager.create_tables()
    twitter_manager.load()
    twitter_manager.run()

    # db = DBManager(config.PostgreSQLConfig)
    # tpcc_manager = TPCC(db)
    # tpcc_manager.create_tables()
    # tpcc_manager.load()
    # tpcc_manager.run()
