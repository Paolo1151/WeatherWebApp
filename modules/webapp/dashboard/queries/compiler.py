from typing import Union

from dataclasses import dataclass
from dataclasses import field

from components import QuerySelect
from components import QueryFrom
from components import QueryJoin
from components import QueryWhere
from components import JoinType
from components import QueryTable

@dataclass
class Query():
    __selects: list[QuerySelect]
    __froms: Union[list[QueryFrom], list[QueryJoin]]
    __wheres: list[QueryWhere]

    def __str__(self):
        template = "SELECT\n\t<selects>\nFROM\n\t<froms>"
        
        for i, select in enumerate(self.__selects):
            suffix = "" if i == len(self.__selects) - 1 else ",\n\t<selects>" 
            statement = str(select)
            template = template.replace(
                            '<selects>',
                            f'{statement}{suffix}'
                        )
        
        for i, frm in enumerate(self.__froms):
            suffix = "" if i == len(self.__froms) - 1 else "\n\t<froms>"
            statement = str(frm)
            template = template.replace(
                            '<froms>',
                            f'{statement}{suffix}'
                        )

        if self.__wheres:
            template += "\nWHERE\n\t<wheres>"
            for i, wheres in enumerate(self.__wheres):
                suffix = "" if i == len(self.__wheres) - 1 else "\n\tAND <wheres>"
                statement = str(wheres)
                template = template.replace(
                                '<wheres>',
                                f'{statement}{suffix}'
                            )

        return template
        

    @property
    def selects(self):
        return self.__selects

    @property
    def froms(self):
        return self.__froms

    @property
    def wheres(self):
        return self.__wheres


class QueryCompiler():

    def compile_queries(self, queries):
        selects = []
        froms = []
        wheres = []
        for query in queries:
            selects += query.selects
            froms += query.froms
            wheres += query.wheres

        # Collate froms (Assume Aliases are correct)
        main_table = list(filter(lambda x: isinstance(x, QueryFrom), froms))[0]
        froms = list(filter(lambda x: isinstance(x, QueryJoin), froms))

        uselects = []
        for select in selects:
            if any(select == oselect for oselect in uselects):
                continue
            uselects.append(select)
             
        ufroms = []
        for frm in froms[1:]:
            if any(frm == ofrm for ofrm in ufroms):
                continue
            ufroms.append(frm)

        ufroms = [main_table] + ufroms
        
        uwheres = []
        for where in wheres:
            if any(where == owhere for owhere in uwheres):
                continue
            uwheres.append(where)

        return Query(uselects, ufroms, uwheres)

if __name__ == '__main__':
    test = Query(
        [
            QuerySelect('avgtemperature', 'c'),
            QuerySelect('region', 's')
        ],
        [
            QueryFrom(QueryTable('weather_schema', 'climate', 'c')),
            QueryJoin(
                QueryTable('weather_schema', 'climate', 'c'), 
                QueryTable('weather_schema', 'station', 's'),
                JoinType.Left,
                'stationid'
            ),
            QueryJoin(
                QueryTable('weather_schema', 'climate', 'c'),
                QueryTable('weather_schema', 'dates', 'd'),
                JoinType.Left,
                'dateid'
            )
        ],
        [
            QueryWhere('d.month = 1')
        ]
    )

    print(str(test))

    test2 = Query(
        [
            QuerySelect('mintemperature', 'c'),
            QuerySelect('avgtemperature', 'c'),
            QuerySelect('maxtemperature', 'c')
        ],
        [
            QueryFrom(QueryTable('weather_schema', 'climate', 'c')),
            QueryJoin(
                QueryTable('weather_schema', 'climate', 'c'), 
                QueryTable('weather_schema', 'station', 's'),
                JoinType.Left,
                'stationid'
            ),
            QueryJoin(
                QueryTable('weather_schema', 'climate', 'c'),
                QueryTable('weather_schema', 'province', 'p'),
                JoinType.Left,
                'provinceid'
            )
        ],
        [

        ]
    )

    print(str(test2))

    compiler = QueryCompiler()
    compiled = compiler.compile_queries([test, test2])

    print(str(compiled))

