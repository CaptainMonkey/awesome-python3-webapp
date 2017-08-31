import asyncio
import logging
import aiomysql

def log(sql,args = ()):
    logging.info('SQL:%s' % sql)

async def create_pool(loop,**kw):#**kw是一个字典dict，表示关键字参数
    logging.info('create database connection pool...')
    global  __pool
    # dict有一个get方法，如果dict中有对应的value值，则返回对应于key的value值，否则返回默认值，例如下面的host，如果dict里面没有
    # 'host',则返回后面的默认值，也就是'localhost'
    __pool = await aiomysql.create_pool(
        host = kw.get('host','localhost'),
        port = kw.get('port',3306),
        user = kw['user'],
        password = kw['passwod'],
        db = kw['db'],
        charset = kw.get('charset','utf-8'),
        autocommi = kw.get('autocummit',True),
        maxsize = kw.get('maxsize','10'),
        minsize = kw.get('minsize',1),
        loop = loop
    )



async def select(sql,args,size = None):
    log(sql,args)
    global __pool
    #Python的with语句是提供一个有效的机制，让代码更简练，同时在异常产生时，清理工作更简单
    #这里给conn赋值，__pool
    with (await __pool) as conn:
        try:
            #数据库连接，使用Connect的cursor()方法获取操作游标对象,游标对象用于执行查询和获取结果
            #await从连接池中返回一个连接， 这个地方已经创建了进程池并和进程池连接了，进程池的创建被封装到了create_pool(loop, **kw)
            cur = await conn.cursor(aiomysql.DictCursor)
            #执行数据库操作,执行一个数据库查询（select）和命令（update、insert、delete）
            await cur.execute(sql.replace('?','%s'),args or())
            if size:
                #获取行数为size的数据
                rs = await cur.fetchmany(size)
            else:
                #获取余下没有遍历的所有数据
                rs = await cur.fetchall()
            await cur.close()
        except BaseException as e:
            raise
        logging.info('rows return:%s'% len(rs))
        return rs

#将update insert delete 都封装在我们自己定义的execute方法里
async def execute(sql,args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?','%s'),args)
            # 最后一次execute*()方法返回数据的行数,如果设置为-1, 意味着既没有结果集，行数也不能确定
            affected = cur.rowcount()
            await cur.close()
        except BaseException as e:
            raise
        return affected

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    #join能将字符串按制定字符分隔，再将其返回成为一个新的字符串，这里使用逗号分隔L（L中都是问号）
    return ','.join(L)

# 定义Field类，负责保存（数据库）表的字段名和字段类型
class Field(object):
    # 表的字段包括名字、类型、是否为表的主键和默认值
    # 类实例创建后会调用__init__()方法，类实例的创建是通过__new()__方法
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        # 返回的是 表名、字段类型，字段名称
        return '<%S,%s:%S>' % (self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
    def __init__(self,name=None,primary_key=False,default = None,ddl = 'varchar(100)'):
        super().__init__(name,ddl,primary_key,default)

class BooleanField(Field):
    def __init__(self,name = None,primary_key = False,default = False):
        super().__init__(name,'boolean',False,default)

class IntegerField(Field):
    def __init__(self,name=None,primary_key=False,default = 0):
        super().__init__(name,'bigint',primary_key,default)

class  FloatField(Field):
    def __init__(self,name = None,primary_key = False,default = 0.0):
        super().__init__(name,'real',primary_key,default)

class TextField(Field):
    def __init__(self,name = None,default = None):
        super().__init__(name,'text',False,default)




# 元类，用于创建类，元类就是创建类这种对象的东西
# 
class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        if name == 'Model':
            return type.__new__(cls,name,bases,attrs)
        tableName = attrs.get('__table__',None) or name
        logging.info('found model:%s(table:%s)' % (name,Model))
        #存放attrs中的数据（估计value是存放user的信息）
        mappings = dict()
        #存放k的值
        fields = []
        primaryKey = None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info('found mapping:%s==>%s'% (k,v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        #将attrs中的已取出来的值删除
        for k in mappings.keys():
            attrs.pop(k)
        #将fields中的变量都加个反引号（``）
        escaped_fields = list(map(lambda f: '`%s`' % f,fields))
        attrs['__mapping__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__field__'] = fields
        attrs['__select__'] = 'select `%s` ,%s from `%s`' %(primaryKey,', '.join(escaped_fields),tableName)
        attrs['__insert__'] = 'insert into `%s` (%s,`%s`) values (%s)' % (tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (tableName,','.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)),primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s` = ?' % (tableName,primaryKey)
        return type.__new__(cls,name,bases,attrs)


class Model(dict,metaclass=ModelMetaclass):
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except:
            raise AttributeError(r'''Model' object has no attribute '%s''' % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value = getattr(self,key,None)
        if value is None:
            #没有搞懂啊，回头再看
            field = self.__mapping__[key]
            if field.default is not None:
                #检查field.default是否可被调用,
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s : %s ' % (key,str(value)))
                setattr(self,key,value)
        return value

    @classmethod
    async def findAll(cls,where=None,args=None,**kw):
        sql= [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        orderBy = kw.get('orderBy',None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit',None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit,int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit,tuple) and len(limit) == 2:
                sql.append('?,?')
                args.extend(limit)
            rs  = await select(' '.join(sql),args)
            return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls,selectField,where = None,args = None):
        sql = ['select %s _num_from `%s`' %(selectField,cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql),args,1)
        if len(rs):
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls,pk):
        ' find object by primary key. '
        rs = await select('%s where `%s` = ?'% (cls.__select__,cls.__primary_key__),[pk],1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__,args)
        if rows !=1:
            logging.warn('failed to remove by primary key: affected rows: %s' %rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__,args)
        if rows !=1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue,self.__fields__))
        rows = await execute(self.__update__,args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)














