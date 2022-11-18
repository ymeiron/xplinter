from lark.lark import Lark, Token
from lark.visitors import Interpreter
from typing import List, Tuple, Optional
from .driver import Driver
from xplinter import Field, Hash_field, Normal_field, Parent_field, Data_type, Cardinality, Entity, Record, View, Kv_extractor, Kv_entity
import importlib, os

class Generator_interp(Interpreter):
    def generator_content(self, tree):
        result = self.visit_children(tree)
        return '(' + ''.join(result) + ')'
    def parens_content(self, tree):
        return ''.join(tree.children)
    def __call__(self, tree) -> List[Tuple[str,str]]:
        results = self.visit(tree)
        if len(results) == 2 and isinstance(results[0], Token): results = [results]
        output = []
        for result in results:
            generator_type, generator_content = result
            generator_type = generator_type.value
            generator_content = generator_content[1:-1]
            output.append((generator_type, generator_content))
        return output

def cardinality_helper(cardinality_string: str) -> Cardinality:
    output = Cardinality.__members__.get(cardinality_string.lower().replace(' ', '_'))
    if not output:
        raise RuntimeError(f'Could not understand cardinality `{cardinality_string}`')
    return output

def type_helper(type_name: str) -> Data_type:
    output = Data_type.__members__.get(type_name.lower())
    if not output:
        raise RuntimeError(f'Could not understand type `{type_name}`')
    return output

def create_field_helper(field_name: str, type_name: str, type_size: int, generator_list: List[Tuple[str,str]], flag: str, globals_dict: dict) -> Field:
    field_type = type_helper(type_name)
    generator_types = [generator[0] for generator in generator_list]
    if generator_types == ['NULL']:
        field = Field(field_name, field_type, type_size)
        return field
    if generator_types == ['HASH']:
        generator_params = generator_list[0][1]
        dependent_field_names = [field_name.strip() for field_name in generator_params.split(',')]
        if type_name != 'BIGINT':
            raise NotImplementedError('Type of a hash field has to be BIGINT')
        field = Hash_field(field_name, Data_type.bigint, dependent_field_names)
        return field
    if generator_types == ['PARENTFIELD']:
        generator_params = generator_list[0][1]
        column_number = int(generator_params)
        field = Parent_field(field_name, column_number, field_type, type_size)
        return field
    optional = flag == 'OPT'
    if generator_types == ['XPATH']:
        xpath = generator_params = generator_list[0][1]
        field = Normal_field(field_name, field_type, xpath, type_size=type_size, optional=optional, func=None)
        return field
    if generator_types == ['XPATH', 'PYTHON']:
        xpath = generator_params = generator_list[0][1]
        func = generator_params = eval(generator_list[1][1], globals_dict)
        field = Normal_field(field_name, field_type, xpath, type_size=type_size, optional=optional, func=func, func_arg_is_text=False)
        return field
    if generator_types == ['XPATH', 'TOTEXT', 'PYTHON']:
        xpath = generator_params = generator_list[0][1]
        func = generator_params = eval(generator_list[2][1], globals_dict)
        field = Normal_field(field_name, field_type, xpath, type_size=type_size, optional=optional, func=func, func_arg_is_text=True)
        return field
    if generator_types == ['PYTHON']:
        func = generator_params = eval(generator_list[0][1], globals_dict)
        field = Normal_field(field_name, field_type, xpath=None, type_size=type_size, optional=optional, func=func)
        return field
    raise RuntimeError(f'Unknown generator pattern for field `{field_name}`.')

class Xplinter_interpreter(Interpreter):
    def __init__(self):
        super().__init__()
        self.generator_interp = Generator_interp()
        self._module_globals: dict = {}
    def __call__(self, tree) -> Record:
        return self.visit(tree)
    def program(self, tree):
        children = self.visit_children(tree)
        record = Record()
        for child in children:
            if isinstance(child, Entity):
                root = child._meta == 'ROOT'
                record.add_entity(child, root=root)
            elif isinstance(child, View):
                child.set_entity
                record.add_view(child)
            else:
                pass
        return record
    def python_module(self, tree):
        module_name = tree.children[0].value.strip()
        module = importlib.import_module(module_name)
        for name in dir(module):
            if not (name.startswith('__') and name.endswith('__')):
                self._module_globals[name] = getattr(module, name)
    def entity_statement(self, tree):
        children = self.visit_children(tree)
        root = children[0] == True
        entity_name = children[1][0].value
        cardinality = cardinality_helper(children[2][0].value)
        parent_name, xpath = None, None
        if children[3]: parent_name = children[3][0].value
        if children[4]:
            xpath = self.generator_interp(children[4])
            if len(xpath) != 1:
                raise RuntimeError('Expected a single XPath expression as third argument of entity')
            xpath = xpath[0]
            if xpath[0] != 'XPATH':
                raise RuntimeError('Expected an XPath expression as third argument of entity')
            xpath = xpath[1]
        field_list = children[5]
        entity = Entity(entity_name, cardinality, xpath, parent_name)
        for field in field_list:
            entity.add_field(field)
        entity._meta = 'ROOT' if root else ''
        return entity
    def root_entity(self, tree):
        return True
    def generator(self, tree):
        return tree
    def field_descriptor(self, tree):
        field_name_node = tree.children[0]
        flag_node       = tree.children[1]
        field_type_node = tree.children[2]
        generator_node  = tree.children[3]

        # Get field name
        field_name = field_name_node.children[0].value

        # Flags
        if flag_node:
            flag = flag_node.children[0].value
        else:
            flag = None

        # Get type name and size (if exists)
        type_name_node  = field_type_node.children[0]
        type_name       = type_name_node.children[0].value
        type_size       = 0
        if len(field_type_node.children) == 2:
            type_size_node = field_type_node.children[1]
            type_size      = int(type_size_node.children[0].value)

        # Deal with the generators (using a separate interpreter class)
        generator_list = self.generator_interp(generator_node)

        field = create_field_helper(field_name, type_name, type_size, generator_list, flag, self._module_globals)
        field._meta = flag

        return field

    def kv_statement(self, tree):
        children = self.visit_children(tree)
        entity_name = children[0][0].value
        kv_descriptor_node = children[1]
        special_field_list = children[2]

        kv_extractor = Kv_extractor()
        for kv_descriptor in kv_descriptor_node:
            key = kv_descriptor[0][0].value
            generator = self.generator_interp(kv_descriptor[1])
            xpath     = generator[0][1]
            kv_extractor.add_xpath(key, xpath)

        kv_entity = Kv_entity(entity_name, Cardinality.one_or_more, parent_name='*root')
        kv_entity.kv_extractor = kv_extractor
        for field in special_field_list:
            special_field_name = field[0]
            special_field_type = field[1]
            kv_entity.add_special_field(special_field_name, special_field_type)

        return kv_entity
    
    def kv_special_field_descriptor(self, tree):
        special_field_name = tree.children[1].children[0].value
        special_field_type = tree.children[0].children[0].value
        return (special_field_name, special_field_type)

    def view_statement(self, tree):
        children    = self.visit_children(tree)
        view_name     = children[0][0].value
        entity_name   = children[1][0].value
        alias_list    = children[2]
        view = View(view_name, entity_name, alias_list)
        return view
    def alias_descriptor(self, tree):
        field_name = tree.children[0].children[0].value
        alias_node = tree.children[1]
        if alias_node:
            alias = alias_node.children[0].value
        else:
            alias = field_name
        return (field_name, alias)

class Generate_record: # Create a singleton object in this module
    def __init__(self):
        dir_name = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_name, 'xplinter.lark'), 'r') as f:
            grammar = f.read()
        self.parser = Lark(grammar, start='program', parser='lalr')
        self.xplinter_interpreter = Xplinter_interpreter()
    def __call__(self, content: str, commit: bool = True, driver: Optional[Driver] = None) -> Record:
        parse_tree = self.parser.parse(content)
        record = self.xplinter_interpreter(parse_tree)
        if commit:
            record.commit()
        record.set_driver(driver)
        return record

if __name__ == '__main__':
    generate_record = Generate_record()
    with open('wos.xplinter', 'r') as f:
        content = f.read()
    record = generate_record(content, commit=False)