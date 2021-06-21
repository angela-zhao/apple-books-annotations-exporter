# -*- coding: utf-8 -*-

import os
import argparse
from glob import glob
import pandas as pd
import sqlite3
from sqlite3 import Error

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--books_db_dir", type=str,
                        default="/Users/angelazhao/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary",
                        help="Directory containing database of books in Apple books")
    parser.add_argument("--notes_db_dir", type=str,
                        default="/Users/angelazhao/Library/Containers/com.apple.iBooksX/Data/Documents/AEAnnotation",
                        help="Directory containing database of notes in Apple books")
    parser.add_argument("--outpath", type=str,
    					default="apple_books_notes.csv",
    					help="Path to output csv of Apple Books annotations")
    args = parser.parse_args()
    return args

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
        
    Arguments:
        db_file (str): database file
    
    Returns:
        Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    conn.text_factory = lambda x: str(x, "utf8")

    return conn


if __name__ == "__main__":
    args = parse_arguments()
    
    try:
        books_db_path = glob(os.path.join(args.books_db_dir, "*.sqlite"))[0]
    except IndexError:
        print("No books found in Apple books library.")
        
    try:
        notes_db_path = glob(os.path.join(args.notes_db_dir, "*.sqlite"))[0]
    except IndexError:
        print("No notes found in Apple books library.")
        
    books_conn = create_connection(books_db_path) 
    table = pd.read_sql_query("SELECT ZASSETID as AssetID, ZTITLE AS Title, ZAUTHOR AS Author, ZCOVERURL as CoverURL, ZGENRE as Genre FROM ZBKLIBRARYASSET WHERE ZTITLE IS NOT NULL", books_conn)
    
    notes_conn = create_connection(notes_db_path) 
    table2 = pd.read_sql_query('''
                               SELECT
				ZANNOTATIONREPRESENTATIVETEXT as BroaderText,
				ZANNOTATIONSELECTEDTEXT as HighlightedText,
				ZANNOTATIONNOTE as Note,
				ZFUTUREPROOFING5 as Chapter,
				ZANNOTATIONCREATIONDATE as Created,
				ZANNOTATIONMODIFICATIONDATE as Modified,
				ZANNOTATIONASSETID,
                ZPLLOCATIONRANGESTART,
                ZANNOTATIONLOCATION
			FROM ZAEANNOTATION
			WHERE ZANNOTATIONSELECTEDTEXT IS NOT NULL
			ORDER BY ZANNOTATIONASSETID ASC,Created ASC
                               ''', notes_conn)
    
    df = pd.merge(table, table2, how="inner", left_on="AssetID", right_on="ZANNOTATIONASSETID")
    df.drop(["ZANNOTATIONASSETID"], axis=1, inplace=True)

    if args.outpath:
    	print("Writing annotations to csv...")
    	df.to_csv(args.outpath, index=False, encoding='utf-8')

