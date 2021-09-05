#!/usr/bin/env python3
# database.py - Interfaces with a database which stores Fidelity ETFs daily
# dollar change amounts

import os
from typing import List, Tuple

import sqlite3

# User-defined Types
FileName = str

DEBUG = True
DATABASE_LOCATION = "funds.db"
INTERVAL = "-1 year"

class FidelityDB:
    def __init__(self, database: FileName, initialize: bool=False):
        self.database = database
        self.connection = sqlite3.connect(database)
        self.cursor = self.connection.cursor()
        if initialize:
            self.initializeDB()
    
    def initializeDB(self) -> None:
        self.destroyDB()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS funds (
                fundID INTEGER PRIMARY KEY ASC,
                fundName TEXT NOT NULL,
                fundLookup TEXT NOT NULL UNIQUE
            );
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS dollarChanges (
                dollarChangeID INTEGER PRIMARY KEY ASC,
                date TEXT NOT NULL,
                fundID INTEGER REFERENCES funds(fundID) ON UPDATE CASCADE ON DELETE CASCADE,
                dollarChange INTEGER NOT NULL   -- Stored as 1000s of a cent, i.e. 1.2050 as 12050
            );
        """)
        self.connection.commit()
        
    def destroyDB(self) -> None:
        self.cursor.execute("""
            DROP TABLE IF EXISTS funds;
        """)
        self.cursor.execute("""
            DROP TABLE IF EXISTS dollarChanges;
        """)
        self.connection.commit()
        
    def close(self) -> None:
        self.connection.close()

    def insertOrIgnoreFund(self, fundName: str, fundLookup: str) -> None:
        self.cursor.execute("""
            INSERT OR IGNORE INTO funds (fundName, fundLookup) VALUES (?, ?);
        """, (fundName, fundLookup))
        self.connection.commit()
       
    def insertOrIgnoreFunds(self, funds: List[Tuple[str, str]]) -> None:
        """Inserts a list of (fundName, fundLookup) tuples"""
    
        self.cursor.executemany("""
            INSERT OR IGNORE INTO funds (fundName, fundLookup) VALUES (?, ?);
        """, (funds))
        self.connection.commit()
        
    def insertDollarChange(self, fundLookup: str, dollarChange: int, date: str=None) -> None:
        """date must be in YYYY-MM-DD format"""
        
        if date is None:
            self.cursor.execute("""
                INSERT INTO dollarChanges (date, fundID, dollarChange)
                VALUES (
                    (SELECT date("now", "localtime")),
                    (SELECT fundID FROM funds WHERE fundLookup = ?),
                    ?
                );
            """, (fundLookup, dollarChange))
        else:
            self.cursor.execute("""
                INSERT INTO dollarChanges (date, fundID, dollarChange)
                VALUES (
                    ?,
                    (SELECT fundID FROM funds WHERE fundLookup = ?),
                    ?
                );
            """, (date, fundLookup, dollarChange))
        self.connection.commit()
        
    def getIntervalToDate(self, interval: str) -> str:
        return self.cursor.execute("""
            SELECT date("now", "{}");
        """.format(INTERVAL), {"interval": interval}).fetchone()[0]
        
    def getDollarChangesIntervalToDateByFund(self, interval: str, fundLookup: str) -> List[Tuple[str, int]]:
        """
        interval must be in a valid SQLite modifier (https://www.sqlite.org/lang_datefunc.html#modifiers)
        
        Returns [(date, dollarChange), ...]
        """
    
        return self.cursor.execute("""
            SELECT date, dollarChange
            FROM dollarChanges
            WHERE
                fundID = (SELECT fundID FROM funds WHERE fundLookup = :fundLookup)
                    AND date >= (SELECT date("now", "-1 year"))
                    AND date <= (SELECT date("now"))
            ORDER BY date ASC;
        """.format(INTERVAL), {"interval": interval, "fundLookup": fundLookup}).fetchall()

if __name__ == "__main__" and DEBUG:
    FidelityDB(DATABASE_LOCATION, initialize=True).close()