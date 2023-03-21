from typing import List, Callable, Optional, Any, Dict, Tuple, Iterable
from .driver import Driver
from enum import Enum
from lxml import etree
import pandas as pd, hashlib, struct, datetime

STRIP_STRINGS: bool = True

class Data_type(Enum):
    smallint = 1
    integer  = 2
    bigint   = 3
    real     = 4
    double   = 5
    text     = 6
    date     = 7
    boolean  = 8
    char     = 9
    xml      = 10
    unknown  = 11

    @staticmethod
    def code(data_type) -> Optional[str]: # Make data_type "Self" type in Python 3.11
        codes = [None, 'h', 'i', 'q', 'f', 'd', None, None, '?', 'c', 'b', None]
        return codes[data_type.value]

def cast(value: str, data_type: Data_type) -> Any:
    if value is None:
        return None
    if data_type in [Data_type.smallint, Data_type.integer, Data_type.bigint]:
        return int(value)
    if (data_type == Data_type.text) or (data_type == Data_type.char):
        if STRIP_STRINGS:
            return value.strip()
        return value
    if (data_type == Data_type.date):
        year  = int(value[:4])
        month = int(value[4:6])
        day   = int(value[6:])
        return datetime.date(year, month, day)
    raise NotImplemented

def xml_serialize(node: etree._Element):
        return etree.tostring(node, method="c14n2", strip_text=True)

def hash_function(data: Iterable, positive: bool = True, full_hex: bool = False):
    m = hashlib.md5()
    for datum in data:
        if   isinstance(datum, str):
            datum_serialized = datum.encode('utf-8')
        elif isinstance(datum, int):
            datum_serialized = datum.to_bytes(8, 'big')
        elif datum is None:
            datum_serialized = b'\0'
        elif isinstance(datum, bytes):
            datum_serialized = datum
        else:
            raise RuntimeError(f'Unexpected type to hash: {type(datum)}')

        m.update(datum_serialized)

    digest = m.digest()

    if full_hex:
        return digest.hex()

    id, _ = struct.unpack('!2q', digest)
    if positive:
        id = abs(id)
    return id

class Field:
    """This class contains just the description of the field and how to obtain the data. No data is actually stored here."""
    def __init__(self, name: str, data_type: Data_type, type_size: int = 0):
        self.name: str            = name
        self.data_type: Data_type = data_type
        self.type_size: int       = type_size
        self._meta: str           = ''
    def process(self, *args, **kwargs) -> Optional[Any]:
        args, kwargs # To prevent the linter from complaining about unused variables
        return None

class Normal_field(Field):
    def __init__(self,
        name: str,
        data_type: Data_type,
        xpath: Optional[str],
        type_size: int = 0,
        optional: bool = False,
        func: Optional[Callable] = None,
        func_arg_is_text: bool = False
    ):
        super().__init__(name, data_type, type_size)
        self._xpath = None
        if xpath:
            try:
                self._xpath = etree.XPath(xpath)
            except etree.XPathSyntaxError:
                raise RuntimeError(f"XPath syntax error in field '{name}': cannot process '{xpath}'")
        self._func = func
        self.optional = optional
        self.func_arg_is_text = func_arg_is_text
    def process(self, node: etree._Element) -> Optional[Any]: # None is part of Any, so Optional[Any] means nothing
        if self._xpath:
            result = self._xpath(node)
            if self.optional and (len(result) == 0):
                if self._func:
                    return self._func(None)
                return None
            if len(result) != 1:
                raise RuntimeError(f'XPATH({self._xpath.path}) for field `{self.name}` expected exactly one result for field but got {len(result)}')
            result = result[0]
            if self.data_type == Data_type.xml:
                return xml_serialize(result)
            if not self._func is None:
                if isinstance(result, etree._Element) and self.func_arg_is_text:
                    result = result.text
                return self._func(result)
            if isinstance(result, etree._Element):
                result = result.text
            return cast(result, self.data_type)
        else:
            if self._func:
                return self._func(node)
            else:
                raise RuntimeError # Shouldn't be here

class Parent_field(Field):
    def __init__(self, name: str, column_number: int, data_type: Data_type, type_size: int = 0):
        super().__init__(name, data_type, type_size)
        self.column_number = column_number


class Hash_field(Field):
    def __init__(self, name: str, data_type: Data_type, fields_to_hash: List[str]):
        super().__init__(name, data_type)
        self.fields_to_hash = fields_to_hash

class Cardinality(Enum):
    one          = 1
    zero_or_one  = 2
    one_or_more  = 3
    zero_or_more = 4

class Entity:
    """Contains a list of field (descriptors) and also holds the data. The data are stored as a list of lists"""
    def __init__(self, name: str, cardinality: Cardinality = Cardinality.one, xpath: str = '', parent_name: str = ''):
        self.name: str = name
        self.cardinality: Cardinality = cardinality
        self.field_list: List[Field] = []
        self.field_indices: Dict[str,int] = {} # do we really need the field itself here?
        self._xpath: Optional[etree.XPath] = None
        if xpath:
            self._xpath = etree.XPath(xpath)
        self.data: List[List] = []
        self.parent_name: str = parent_name
        self.children: List[Entity] = [] # To be filled in later 
        self.hash_field_order: List[int] = []
        self._meta: str = ''
    def reset(self):
         self.data = []
    def calc_hash_field_order(self):
        hash_field_order: List[str] = []
        def walk(field: Field):
            if not isinstance(field, Hash_field): return
            if field.name in hash_field_order: return
            for child_name in field.fields_to_hash:
                child_field: Field = self.get_field(child_name)
                walk(child_field)
            hash_field_order.append(field.name)
        for field in self.field_list:
            walk(field)
        self.hash_field_order = list(map(self.get_field_index, hash_field_order))
        for field in self.field_list:
            walk(field)
    def add_field(self, field: Field):
        if field.name in self.field_indices.keys():
            raise RuntimeError(f'Field `{field.name}` already exists in entity `{self.name}`')
        self.field_list.append(field)
        my_idx = len(self.field_list) - 1
        self.field_indices[field.name] = my_idx
    def get_field_index(self, field_name: str) -> int:
        return self.field_indices[field_name]
    def get_field(self, field_name: str) -> Field:
        return self.field_list[self.field_indices[field_name]]
    def get_node_list(self, tree: etree._Element):
        if self._xpath:
            return self._xpath(tree)
        return [tree]
    def process(self, tree, parent_data: Any = None):
        node_list = self.get_node_list(tree)
        if (self.cardinality == Cardinality.one) and (len(node_list) != 1):
            raise RuntimeError(f'Entity `{self.name}` expected exactly one item but got {len(node_list)}')
        for node in node_list:
            row = []
            for field in self.field_list:
                value = field.process(node)
                if (not value is None) and ((field.data_type == Data_type.text) or (field.data_type == Data_type.char)) and (field.type_size > 0):
                    if len(value) > field.type_size:
                        raise RuntimeError(f'Value of field `{field.name}` is "{value}" and too long (maximum {field.type_size} characters)')
                if isinstance(field, Parent_field):
                    value = parent_data[field.column_number]
                row.append(value)
            # Hash fields last
            # we can just get the hash field and dependent field just once in the beginning instead of every time we process the data
            for idx in self.hash_field_order:
                field = self.field_list[idx]
                data_to_hash = []
                for other_field_name in field.fields_to_hash:
                    other_idx = self.get_field_index(other_field_name)
                    data_to_hash.append(row[other_idx])
                row[idx] = hash_function(data_to_hash)
            self.data.append(row)
            for child_entity in self.children:
                child_entity.process(node, row)
    def to_dataframe(self, expose_hidden_fields: bool = False):
        columns = [field.name for field in self.field_list]
        df = pd.DataFrame(self.data, columns=columns)
        if not expose_hidden_fields:
            for column in columns:
                if column.startswith('*'):
                    del df[column]
        return df

class View:
    def __init__(self, name: str, entity_name: str, fields: List[Tuple[str,str]]):
        self.name: str = name
        self.data: List[Any]    = []
        self.indices: List[int] = []
        self.columns: List[str] = []
        self._fields: List[Tuple[str,str]] = fields

        self.entity: Optional[Entity]   = None
        self.entity_name: str = entity_name
    def reset(self):
         self.data = []
    def copy_data_from_entity(self):
        if self.entity is None:
            raise RuntimeError
        for row in self.entity.data:
            new_row = [row[i] for i in self.indices]
            self.data.append(new_row)
    def to_dataframe(self):
        df = pd.DataFrame(self.data, columns=self.columns)
        return df
    def set_entity(self, entity: Entity):
        if self.entity:
            raise RuntimeError(f'View `{self.name}` already has an entity set')
        self.entity = entity
        for source_field_name, destination_field_name in self._fields:
            self.indices.append(entity.get_field_index(source_field_name))
            self.columns.append(destination_field_name)

class Kv_extractor:
    def __init__(self):
        self.key_xpath: Dict[str,etree.XPath] = {}
        self.data: List[Tuple[str,str]] = []
    def add_xpath(self, key, xpath):
        self.key_xpath[key] = etree.XPath(xpath)
    def process(self, tree: etree._Element):
        for key, xpath in self.key_xpath.items():
            result_list = xpath(tree)
            for result in result_list:
                if isinstance(result, etree._Element):
                    result = result.text
                self.data.append((key, result))
    def reset(self):
        self.data = []

class Kv_entity(Entity):
    def add_field(self, field: Field):
        return NotImplementedError
    def add_kv_extractor(self, kv_extractor: Kv_extractor):
        self.kv_extractor = kv_extractor
    def add_special_field(self, name: str, sepcial_type: str):
        super().add_field(Field(name, Data_type.unknown))
        self.field_list[-1]._meta = sepcial_type
    def process(self, tree, parent_data: Any = None):
        node_list = self.get_node_list(tree)
        if len(node_list) != 1:
            raise RuntimeError(f'Entity `{self.name}` expected exactly one item but got {len(node_list)}')
        node = node_list[0]
        self.kv_extractor.process(node)

        for kv in self.kv_extractor.data:
            row = []
            for field in self.field_list:
                if   field._meta == 'KEY':
                    datum = kv[0]
                elif field._meta == 'VALUE':
                    datum = kv[1]
                elif field._meta == 'HASH':
                    datum = hash_function(kv)
                elif field._meta.startswith('PARENTFIELD'):
                    idx = int(field._meta[12:-1])
                    datum = parent_data[idx]
                else:
                    raise NotImplementedError
                row.append(datum)
            self.data.append(row)
    def reset(self):
        super().reset()
        self.kv_extractor.reset()

class Record:
    def __init__(self, driver: Optional[Driver] = None):
        self.entity_dict: Dict[str,Entity] = {}
        self.root: Optional[Entity] = None
        self.view_dict: Dict[str,View] = {}
        self.committed: bool = False
        self.set_driver(driver)
    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.close()
    def set_driver(self, driver: Optional[Driver]):
        if driver:
            driver.set_record(self)
            driver.open()
            self.driver = driver
    def reset(self):
        for entity in self.entity_dict.values():
            entity.reset()
        for view in self.view_dict.values():
            view.reset()
    def add_entity(self, entity: Entity, root: bool = False) -> None:
        # TODO check that all entities that were added descend from the root entity
        if self.committed:
            raise RuntimeError(f'Record already committed, cannot add new entities')
        if entity.name in self.entity_dict.keys():
            raise RuntimeError(f'Entity `{entity.name}` already exists')
        self.entity_dict[entity.name] = entity
        if root:
            self.root_entity = entity
    def add_view(self, view: View) -> None:
        if self.committed:
            raise RuntimeError(f'Record already committed, cannot add new views')
        self.view_dict[view.name] = view
    def commit(self):
        for child_entity in self.entity_dict.values():
            if not child_entity.parent_name: continue
            parent_entity = self.entity_dict[child_entity.parent_name]
            parent_entity.children.append(child_entity)
        for entity in self.entity_dict.values():
            entity.calc_hash_field_order()
        for view in self.view_dict.values():
            entity_name = view.entity_name
            entity = self.entity_dict.get(entity_name)
            if not entity:
                raise RuntimeError(f'View `{view.name}` depends on entity `{entity_name}`, but none named such was found in record')
            view.set_entity(entity)
        self.committed = True
    def process(self, tree):
        self.root_entity.process(tree)
        for view in self.view_dict.values():
            view.copy_data_from_entity()
    def write(self, reset=True):
        if not hasattr(self, 'driver'):
            raise RuntimeError('Cannot write because no writing driver was set')
        self.driver.write()
        self.reset()


if __name__ == '__main__':
    with open('sample.xml', 'rb') as f:
        tree = etree.fromstring(f.read(), None)

    record = Record()

    publication = Entity('publication')
    publication.add_field(Normal_field('uid', Data_type.text, 'uid'))
    publication.add_field(Normal_field('year', Data_type.integer, 'info/@year'))
    publication.add_field(Normal_field('title', Data_type.text, 'title'))
    record.add_entity(publication, root=True)

    conference = Entity('conference', Cardinality.one_or_more, 'conferences/conference', parent_name = 'publication')
    conference.add_field(Normal_field('uid', Data_type.text,  'uid'))
    conference.add_field(Normal_field('name', Data_type.text, 'name'))
    conference.add_field(Normal_field('date', Data_type.text, 'date'))
    conference.add_field(Normal_field('city', Data_type.text, 'city'))
    record.add_entity(conference)

    conference_sponsor = Entity('conference_sponsor', Cardinality.zero_or_more, 'sponsor', parent_name = 'conference')
    conference_sponsor.add_field(Normal_field('name', Data_type.text, 'name'))
    conference_sponsor.add_field(Normal_field('established', Data_type.text, 'established', optional=True))
    conference_sponsor.add_field(Parent_field('conf_id', Data_type.text))
    conference_sponsor.add_field(Hash_field('zzz1', Data_type.bigint, ['name', 'generated_uid']))
    conference_sponsor.add_field(Hash_field('generated_uid', Data_type.bigint, ['name', 'established']))
    conference_sponsor.add_field(Hash_field('zzz2', Data_type.bigint, ['name', 'generated_uid']))

    
    record.add_entity(conference_sponsor)

    record.commit()

    record.process(tree)

    for entity_name, entity in record.entity_dict.items():
        print('---------------------------')
        print('=== ' + entity_name.upper() + ' ===')
        print(entity.to_dataframe())
        