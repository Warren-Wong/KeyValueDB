#-*- coding: utf-8 -*-

import sqlite3
import base64
import hashlib

class Table(object):   
    def __init__(self, dbpath, table, membersql):
        self.dbpath = dbpath
        self.table = table
        if self.table_exist() != True:
            self.create(membersql)
        self.member = self.table_member()

    def execute(self, sqlcmd):
        open_flag = False
        sqlerr = None
        try:
            conn = sqlite3.connect(self.dbpath)
            open_flag = True
            res = conn.execute(sqlcmd).fetchall()
            conn.commit()
            return (True,res)
        except Exception as ex:
            sqlerr = "<CMD>" + sqlcmd + "\n<ERR> " + str(ex)
        finally:
            if open_flag == True:
                conn.close()
        return (False,sqlerr)

    def table_exist(self):
        cmd = "SELECT COUNT(*) FROM sqlite_master where type='table' and name='%s';"%(self.table)
        stat,res = self.execute(cmd)
        if stat == True:
            if res[0][0] == 1:
                return True
            else:
                return False
        else:
            raise Exception(str(res))

    def table_member(self):
        cmd = "SELECT * FROM sqlite_master where type='table' and name='%s';"%(self.table)
        stat,res = self.execute(cmd)
        if stat == True and len(res)==1:
            sql = res[-1][-1]
            sql = sql[sql.find('(')+1:sql.find(')')]
            member = []
            for m in sql.split(','):
                m = m.strip()
                m = m[:m.find(' ')]
                member.append(m)
            return member
        else:
            raise Exception(str(res))

    def e(self,v):
        if type(v) == type(1):
            return "%d"%(v)
        elif type(v) == type('') or type(v) == type(u''):
            return "'%s'"%( base64.b64encode(v) )
        elif type(v) == type(None):
            return None
        else:
            raise Exception("value type error: type == %s"%(type(v)) )

    def d(self,v):
        if type(v) == type('') or type(v) == type(u''):
            return "%s"%(  base64.b64decode(v)  )
        else:
            return v

    def encode(self, entry):
        en_entry = {}
        for k in self.member:
            en_entry[k] = self.e(entry[k])
        return en_entry

    def decode(self, entry):
        de_entry = {}
        for k in self.member:
            de_entry[k] = self.d(entry[k])
        return de_entry

    def fetch_entry(self, unformat_entry):
        entry = self.get_empty_entry()
        for k in self.member:
            if k in unformat_entry:
                entry[k] = unformat_entry[k]
        return entry

    def get_empty_entry(self):
        entry = {}
        for i in self.member:
            entry[i] = None
        return entry

    def get_entry_list(self, exec_resoult, member_list=None):
        if member_list == None:
            member_list = self.member
        entry_list = []
        for it in exec_resoult:
            entry = self.get_empty_entry()
            tmp_entry = dict( map( lambda x,y: (x, y), member_list, it) )
            entry.update(tmp_entry)
            entry_list.append( self.decode(entry) )
        return entry_list

    def create(self, membersql):
        cmd =  "CREATE TABLE IF NOT EXISTS %s ( %s );"%(self.table,membersql)
        stat,res = self.execute(cmd)
        if stat == False:
            raise Exception(str(res))

    def select_specific_member(self, member_list, cdt_entry=None, limit=None, offset=None, distinct=False):
        cmd = " SELECT "

        if distinct == True:
            cmd += "DISTINCT "

        cmd += " %s FROM %s "%(' , '.join(member_list), self.table)

        if cdt_entry != None:
            cdt_entry = self.encode(cdt_entry)
            cdt_str = []
            for k in self.member:
                if cdt_entry[k] != None:
                    cdt_str.append( " %s = %s "%( k, cdt_entry[k]) )
            if len(cdt_str) > 0:
                 cmd += " WHERE %s "%(' and '.join(cdt_str))

        if limit != None:
            cmd += " LIMIT %d "%(limit)

        if offset != None:
            cmd += " OFFSET %d "%(offset)

        cmd += " ;"

        stat,res = self.execute(cmd)
        if stat == True:
            return self.get_entry_list(res,member_list)
        else:
            return None

    def select(self, cdt_entry=None, limit=None, offset=None, distinct=False):
        return self.select_specific_member( self.member, cdt_entry, limit, offset, distinct)

    def insert(self, entry):
        mstr = []
        vstr = []
        entry = self.encode(entry)
        for k in self.member:
            if entry[k] != None:
                mstr.append(k)
                vstr.append(entry[k])
        cmd =  "INSERT INTO %s (%s) VALUES (%s) ;"%(self.table, ','.join(mstr), ','.join(vstr))

        stat,res = self.execute(cmd)
        if stat == True:
            return True
        else:
            return False
    def update(self, cdt_entry, entry):
        cdt_entry = self.encode(cdt_entry)
        entry = self.encode(entry)

        cdt_str = []
        set_str = []

        for k in self.member:
            if cdt_entry[k] != None:
                entry[k] = None
                cdt_str.append( " %s = %s "%( k, cdt_entry[k]) )
            if entry[k] != None:
                set_str.append( " %s = %s "%( k, entry[k]) )

        cmd =  "UPDATE %s SET %s WHERE %s ;"%( self.table, ','.join(set_str), ' and '.join(cdt_str))

        stat,res = self.execute(cmd)
        if stat == True:
            return True
        else:
            return False

    def delete(self, cdt_entry):
        cdt_entry = self.encode(cdt_entry)

        cdt_str = []

        for k in self.member:
            if cdt_entry[k] != None:
                cdt_str.append( " %s = %s "%( k, cdt_entry[k]) )

        cmd =  "DELETE FROM %s WHERE %s ;"%( self.table, ','.join(cdt_str))

        stat,res = self.execute(cmd)
        if stat == True:
            return True
        else:
            return False

    def count(self, cdt_entry=None, limit=None, offset=None):

        cmd = "SELECT COUNT(*) FROM %s "%( self.table)

        if cdt_entry != None:
            cdt_entry = self.encode(cdt_entry)
            cdt_str = []
            for k in self.member:
                if cdt_entry[k] != None:
                    cdt_str.append( " %s = %s "%( k, cdt_entry[k]) )
            if len(cdt_str) > 0:
                 cmd += " WHERE %s "%(' and '.join(cdt_str))

        if limit != None:
            cmd += "LIMIT %d "%(limit)

        if offset != None:
            cmd += "OFFSET %d "%(offset)

        cmd += " ;"

        stat,res = self.execute(cmd)
        if stat == True:
            if any( res):
                return res[0][0]
            else:
                return 0
        else:
            return None

    def count_all(self):
        cmd =  "SELECT count(*) FROM %s ;"%(self.table)
        stat,res = self.execute(cmd)
        if stat == True:
            if any( res):
                return res[0][0]
            else:
                return 0
        else:
            return None

    def select_all(self):
        cmd =  "SELECT * FROM %s ;"%(self.table)
        stat,res = self.execute(cmd)
        if stat == True:
            return self.get_entry_list(res)
        else:
            return None

    def select_some(self,limit,offset):
        cmd =  "SELECT * FROM %s "%(self.table)
        cmd += "LIMIT %d OFFSET %d ;"%(limit,offset)
        stat,res = self.execute(cmd)
        if stat == True:
            return self.get_entry_list(res)
        else:
            return None

class KeyValueTable(Table):
    membersql = """ key TEXT PRIMARY KEY NOT NULL,
                    value TEXT"""
    def __init__(self, database_path, table_name):
        super(KeyValueTable,self).__init__( database_path, table_name, self.membersql)

    def set(self, k, v):
        cdt_entry = self.get_empty_entry()
        cdt_entry['key'] = k

        entry = self.get_empty_entry()
        entry['key'] = k
        entry['value'] = v

        if self.count(cdt_entry) == 0:
            return self.insert( entry)
        else:
            return self.update( cdt_entry, entry)

    def get(self, k=None):
        cdt_entry = self.get_empty_entry()
        cdt_entry['key'] = k
        if self.count(cdt_entry) != 0:
            return self.select( cdt_entry, 1, 0)[0]['value']
        return None

    def remove(self, k):
        cdt_entry = self.get_empty_entry()
        cdt_entry['key'] = k
        return self.delete( cdt_entry)

class KeyValueDB:
    def __init__(self,database_path):
        self.table = KeyValueTable(database_path,'key_value_table')
    def set(self,k,v):
        return self.table.set(k,v)
    def get(self,k):
        return self.table.get(k)
    def remove(self,k):
        return self.table.remove(k)
