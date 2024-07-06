import base64
import threading
import queue

from utils import *
from benchmark.twitter.twitter_config import *
from benchmark.application import Application


class TwitterApp(Application):
    def __init__(self, id, conn, queue: queue.PriorityQueue, lock: threading.Condition, terminal_manager):
        super().__init__(id, conn, queue, lock, terminal_manager, Config)

    def follow(self, record):
        # input data
        # src follow dest
        src_id = record.src_id
        dest_id = record.dest_id
        time = record.time
        follow = record.follow
        # transaction start
        transaction = self.conn.begin()
        # check user_id
        key = encode_key(src_id)
        value = self.conn.get(Users.table, key)
        if value is None:
            self.conn.commit()
            return transaction
        if value is False:  # abort
            return transaction
        key = encode_key(dest_id)
        value = self.conn.get(Users.table, key)
        if value is None or value is False:
            self.conn.commit()
            return transaction
        if value is False:  # abort
            return transaction
        # update followers, add src to dest's followers' list/ remove src from dest's followers' list
        key = encode_key(dest_id, src_id)
        value = encode_value(Followers.columns, time, follow)
        ret_value = self.conn.get(Followers.table, key)
        if ret_value is None:  # need to insert
            self.conn.insert(Followers.table, key, value)
        if value is False:  # abort
            return transaction
        else:
            if self.conn.set(Followers.table, key, value) is False:
                return transaction
        # update following, add dest to src's following list/ remove dest from src's following list
        key = encode_key(src_id, dest_id)
        value = encode_value(Following.columns, time, follow)
        ret_value = self.conn.get(Following.table, key)
        if ret_value is None:  # need to insert
            self.conn.insert(Following.table, key, value)
        if ret_value is False:  # abort
            return transaction
        else:
            self.conn.set(Following.table, key, value)
        # update src's follow_list data(set bit = 1/0)
        key = encode_key(src_id)
        value = self.conn.get(FollowList.table, key)
        if value is False:  # abort
            return transaction
        values = decode_value(value)  # dict
        bytes = bytearray(base64.b64decode(values["data"]))
        if follow:
            set_bit_map_at(bytes, dest_id-1)
        else:
            clear_bit_map_at(bytes, dest_id-1)
        data = base64.b64encode(bytes).decode('utf-8')
        value = encode_value(FollowList.columns, data)
        if self.conn.set(FollowList.table, key, value) is False:
            return transaction
        # transaction end
        self.conn.commit()
        return transaction

    def new_tweet(self, record):
        # input data
        user_id = record.user_id
        tweet_id = record.tweet_id
        data = record.data
        # transaction start
        transaction = self.conn.begin()
        # insert into tweet
        key = encode_key(tweet_id)
        value = encode_value(Tweet.columns, user_id, data)
        if self.conn.insert(Tweet.table, key, value) is False:
            return transaction
        # update last_tweet
        key = encode_key(user_id)
        value = encode_value(LastTweet.columns, tweet_id)
        if self.conn.set(LastTweet.table, key, value) is False:
            return transaction
        # transaction end
        self.conn.commit()
        return transaction

    def show_follow(self, record):
        # input data
        user_id = record.user_id
        # transaction start
        transaction = self.conn.begin()
        # get follow_list
        key = encode_key(user_id)
        value = self.conn.get(FollowList.table, key)
        if value is False:
            return transaction
        # not necessary:f
        # if value is not None:
        #     values = decode_value(value)
        #     data = values["data"]
        #     bytes = base64.b64decode(data)
        #     follow_id = []
        #     for i in range(len(bytes)*8):
        #         if get_bit_map_at(bytes, i) == 1:
        #             follow_id.append(i)
        #             if (len(follow_id) >= 20):
        #                 break
        # transaction end
        self.conn.commit()
        return transaction

    def show_tweets(self, record):
        # input data
        user_id = record.user_id
        # transaction start
        transaction = self.conn.begin()
        # get last tweet
        key = encode_key(user_id)
        value = self.conn.get(LastTweet.table, key)
        if value is False:
            return transaction
        if value is None:
            self.conn.commit()
            return transaction
        last_tweet_id = decode_value(value)["last_tweet_id"]
        # get tweets
        for tweet_id in range(max(1, last_tweet_id-10), last_tweet_id+1):
            key = encode_key(tweet_id)
            self.conn.get(Tweet.table, key)
        # transaction end
        self.conn.commit()
        return transaction

    def timeline(self, record):
        # input data
        user_id = record.user_id
        # transaction start
        transaction = self.conn.begin()
        # get follow
        key = encode_key(user_id)
        value = self.conn.get(FollowList.table, key)
        if value is None:
            self.conn.commit()
            return transaction
        if value is False:
            return transaction
        values = decode_value(value)  # dict
        bytes = bytearray(base64.b64decode(values["data"]))
        counter = 0
        for follow_id in range(len(bytes)*8):
            if get_bit_map_at(bytes, follow_id) == 1:
                # get follow_id's last tweet_id
                key = encode_key(follow_id+1)
                value = self.conn.get(LastTweet.table, key)
                if value is False:
                    return transaction
                if value is None:
                    self.conn.commit()
                    return transaction
                last_tweet_id = decode_value(value)["last_tweet_id"]
                # get tweet
                key = encode_key(last_tweet_id)
                if self.conn.get(Tweet.table, key) is False:
                    return transaction
                counter += 1
                if counter == 20:
                    break
        # transaction end
        self.conn.commit()
        return transaction
