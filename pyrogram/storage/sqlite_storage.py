#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

import inspect
import sqlite3
import time
from typing import List, Tuple, Any

from pyrogram import raw
from .storage import Storage
from .. import utils

# language=SQLite
SCHEMA = """
CREATE TABLE sessions
(
    dc_id     INTEGER PRIMARY KEY,
    api_id    INTEGER,
    test_mode INTEGER,
    auth_key  BLOB,
    date      INTEGER NOT NULL,
    user_id   INTEGER,
    is_bot    INTEGER
);

CREATE TABLE peers
(
    id             INTEGER PRIMARY KEY,
    access_hash    INTEGER,
    type           INTEGER NOT NULL,
    username       TEXT,
    phone_number   TEXT,
    last_update_on INTEGER NOT NULL DEFAULT (CAST(STRFTIME('%s', 'now') AS INTEGER))
);

CREATE TABLE version
(
    number INTEGER PRIMARY KEY
);

CREATE INDEX idx_peers_id ON peers (id);
CREATE INDEX idx_peers_username ON peers (username);
CREATE INDEX idx_peers_phone_number ON peers (phone_number);

CREATE TRIGGER trg_peers_last_update_on
    AFTER UPDATE
    ON peers
BEGIN
    UPDATE peers
    SET last_update_on = CAST(STRFTIME('%s', 'now') AS INTEGER)
    WHERE id = NEW.id;
END;
"""


def get_input_peer(peer_id: int, access_hash: int, peer_type: str):
    if peer_type in ["user", "bot"]:
        return raw.types.InputPeerUser(
            user_id=peer_id,
            access_hash=access_hash
        )

    if peer_type == "group":
        return raw.types.InputPeerChat(
            chat_id=-peer_id
        )

    if peer_type in ["channel", "supergroup"]:
        return raw.types.InputPeerChannel(
            channel_id=utils.get_channel_id(peer_id),
            access_hash=access_hash
        )

    raise ValueError(f"Invalid peer type: {peer_type}")

from typing import Tuple, List, Optional


class State:
    """
    Represents the update state data model.

    Attributes:
        id (int): The ID of the state. (0 is me state)
        pts (int): The PTS value.
        date (int): The date value.
        qts (int): The QTS value.
        seq (int): The SEQ value.
    """
    def __init__(self, id: int, pts: int, date: int, qts: Optional[int],  seq: Optional[int]):
        self.id = id 
        self.pts = pts
        self.date = date
        self.qts = qts
        self.seq = seq

    @classmethod
    def default(cls, id: int):
        return cls(id, 1, int(time.time()), None, None)



class StateMixin:
    conn: sqlite3.Connection

    def create_or_exists_table_state(self):
        with self.conn:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS state (
                    id   INTEGER PRIMARY KEY,
                    pts  INTEGER,
                    date INTEGER,
                    qts  INTEGER,
                    seq  INTEGER
                )
            """)

    def get_state(self, id: int) -> State:
        """
        Fetch a specific state by ID from the database.
        If the state is not found, a default state is returned.

        Args:
            id (int): The ID of the state to retrieve.

        Returns:
            Optional[State]: A State object if found, otherwise None.
        """
        query = "SELECT id, pts, date, qts, seq FROM state WHERE id = ?"
        cursor = self.conn.execute(query, (id,))
        row = cursor.fetchone()

        if row is None:
            return State.default(id)

        return State(*row)

    def update_state(self, id: int, pts: int, date: Optional[int], qts: Optional[int] = None,  seq: Optional[int] = None):
        """
        Insert or update a state entry using REPLACE INTO (upsert behavior).

        Args:
            id (int): The ID of the state.
            pts (int): The PTS value.
            date (int): The date value (e.g., Unix timestamp).
            qts (int): The QTS value.
            seq (int): The sequence number.
        """
        if date is None:
            date = int(int(time.time()))
        query = """
            REPLACE INTO state (id, pts, date, qts, seq)
            VALUES (?, ?, ?, ?, ?)
        """
        self.conn.execute(query, (id, pts, date, qts, seq))
        self.conn.commit()

    def reset_state(self, id: int):
        """
        Reset a state entry by ID.
        Instead of deleting the row, set pts = 1.

        Args:
            id (int): The ID of the state to reset.
        """
        query = "UPDATE state SET pts = 1 WHERE id = ?"
        self.conn.execute(query, (id,))
        self.conn.commit()



class SQLiteStorage(Storage, StateMixin):
    VERSION = 3
    USERNAME_TTL = 8 * 60 * 60

    conn: sqlite3.Connection

    def __init__(self, name: str):
        super().__init__(name)

    def create(self):
        with self.conn:
            self.conn.executescript(SCHEMA)

            self.conn.execute(
                "INSERT INTO version VALUES (?)",
                (self.VERSION,)
            )

            self.conn.execute(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)",
                (2, None, None, None, 0, None, None)
            )

        self.create_or_exists_table_state()

    async def open(self):
        raise NotImplementedError

    async def save(self):
        await self.date(int(time.time()))
        self.conn.commit()

    async def close(self):
        self.conn.close()

    async def delete(self):
        raise NotImplementedError

    async def update_peers(self, peers: List[Tuple[int, int, str, str, str]]):
        self.conn.executemany(
            "REPLACE INTO peers (id, access_hash, type, username, phone_number)"
            "VALUES (?, ?, ?, ?, ?)",
            peers
        )

    async def get_peer_by_id(self, peer_id: int):
        r = self.conn.execute(
            "SELECT id, access_hash, type FROM peers WHERE id = ?",
            (peer_id,)
        ).fetchone()

        if r is None:
            raise KeyError(f"ID not found: {peer_id}")

        return get_input_peer(*r)

    async def get_peer_by_username(self, username: str):
        r = self.conn.execute(
            "SELECT id, access_hash, type, last_update_on FROM peers WHERE username = ?"
            "ORDER BY last_update_on DESC",
            (username,)
        ).fetchone()

        if r is None:
            raise KeyError(f"Username not found: {username}")

        if abs(time.time() - r[3]) > self.USERNAME_TTL:
            raise KeyError(f"Username expired: {username}")

        return get_input_peer(*r[:3])

    async def get_peer_by_phone_number(self, phone_number: str):
        r = self.conn.execute(
            "SELECT id, access_hash, type FROM peers WHERE phone_number = ?",
            (phone_number,)
        ).fetchone()

        if r is None:
            raise KeyError(f"Phone number not found: {phone_number}")

        return get_input_peer(*r)

    def _get(self):
        attr = inspect.stack()[2].function

        return self.conn.execute(
            f"SELECT {attr} FROM sessions"
        ).fetchone()[0]

    def _set(self, value: Any):
        attr = inspect.stack()[2].function

        with self.conn:
            self.conn.execute(
                f"UPDATE sessions SET {attr} = ?",
                (value,)
            )

    def _accessor(self, value: Any = object):
        return self._get() if value == object else self._set(value)

    async def dc_id(self, value: int = object):
        return self._accessor(value)

    async def api_id(self, value: int = object):
        return self._accessor(value)

    async def test_mode(self, value: bool = object):
        return self._accessor(value)

    async def auth_key(self, value: bytes = object):
        return self._accessor(value)

    async def date(self, value: int = object):
        return self._accessor(value)

    async def user_id(self, value: int = object):
        return self._accessor(value)

    async def is_bot(self, value: bool = object):
        return self._accessor(value)

    def version(self, value: int = object):
        if value == object:
            return self.conn.execute(
                "SELECT number FROM version"
            ).fetchone()[0]
        else:
            with self.conn:
                self.conn.execute(
                    "UPDATE version SET number = ?",
                    (value,)
                )
