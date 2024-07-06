from database.database import DBManager
from benchmark.blind.blind_config import *
from benchmark.blind.blind_load import Loader
from benchmark.blind.blind_terminal import *
from benchmark.blind.blind_app import *
from utils import SharedInt
from benchmark.scheduler import Scheduler
from benchmark.terminal import TerminalManager
from benchmark.application import ApplicationManager


class Blind:
    def __init__(self, database: DBManager, weight_read=50, wight_update=50):
        self.db = database
        Config.weight_read = weight_read
        Config.weight_update = wight_update

    def drop_tables(self):
        conn = self.db.connect()
        conn.drop_table(BlindTable.table)
        self.db.close()

    def create_tables(self):
        self.drop_tables()
        conn = self.db.connect()
        conn.create_table(BlindTable.table, 20, 200)
        self.db.close()

    def load(self):
        print("Start Load Blind...")
        shared_key = SharedInt(max_value=Config.num_keys)
        loaders = []
        for _ in range(Config.num_loaders):
            conn = self.db.connect(init=True)
            loader = Loader(conn, shared_key)
            loaders.append(loader)
            loader.start()
        for loader in loaders:
            loader.join()
        self.db.close()
        print("Load Blind Done")

    def run(self):
        print("Start run Blind benchmark...")
        print(
            f"Read: {Config.weight_read} % - Update: {Config.weight_update} %")
        generator = BlindGenerator()
        scheduler = Scheduler()
        term_manager = TerminalManager(
            Config, scheduler, generator, BlindTerminal)
        app_manager = ApplicationManager(
            Config, self.db, term_manager, BlindApp)
        scheduler.set_app(app_manager)

        scheduler.start()
        term_manager.start()
        app_manager.start()

        scheduler.join()
        term_manager.join()
        app_manager.join()
        self.db.close()
        print("Blind Benchmark Done")
