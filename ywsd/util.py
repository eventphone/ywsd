from importlib import import_module


def class_from_dotted_string(dotted_string):
    try:
        module_name, class_name = dotted_string.rsplit('.', 1)
    except ValueError as e:
        raise ImportError("Invalid dotted string: {}".format(dotted_string))

    module = import_module(module_name)

    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError("There is no class {} in module {}".format(class_name, module_name))
