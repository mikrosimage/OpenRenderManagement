'''
Created on Nov 2, 2009

@author: Olivier Derpierre
'''


class Field(object):

    def __init__(self, allow_null=False):
        self.name = None
        self.allow_null = allow_null

    def contribute_to_instance(self, instance):
        pass

    def to_json(self, instance):
        return getattr(instance, self.name)

    def validate_instance(self, instance):
        if not hasattr(instance, self.name):
            raise ValueError("Instance is missing field %s" % self.name)
        if getattr(instance, self.name) is None and not self.allow_null:
            raise ValueError("None is not a valid %s value" % self.name)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)


class ModelType(type):

    def __new__(cls, clsname, bases, attributes):
        fields = {}
        for base in reversed(bases):
            if isinstance(base, ModelType):
                fields.update(base.FIELDS)
        newfields = dict([(name, value) for (name, value) in attributes.items() if isinstance(value, Field)])
        fields.update(newfields)
        for (name, field) in newfields.items():
            field.name = name
            del attributes[name]
        attributes['FIELDS'] = fields
        attributes['changeListeners'] = []
        return super(ModelType, cls).__new__(cls, clsname, bases, attributes)

    def __call__(self, *args, **kwargs):
        instance = super(ModelType, self).__call__(*args, **kwargs)
        instance._changeReady = True
        self.fireCreationEvent(instance)
        return instance


class Model(object):

    __metaclass__ = ModelType

    id = Field()

    def __init__(self, **kwargs):
        self._changeReady = False
        for (key, value) in kwargs.items():
            if key in self.FIELDS:
                setattr(self, key, value)
        for value in self.FIELDS.values():
            value.contribute_to_instance(self)
        self.changeListeners = []

    def __setattr__(self, name, value):
        if hasattr(self, name) and getattr(self, name) == value:
            return
        oldvalue = getattr(self, name, None)
        super(Model, self).__setattr__(name, value)
        if name in self.FIELDS:
            try:
                self.fireChangeEvent(self, name, oldvalue, value)
            except Exception:
                import logging
                logging.getLogger("main.model").exception("error while running event listener")

    def to_json(self):
        self.validate()
        return dict((field.name, field.to_json(self)) for field in self.FIELDS.values())

    def validate(self):
        for field in self.FIELDS.values():
            field.validate_instance(self)

    @classmethod
    def fireCreationEvent(cls, obj):
        for base in obj.__class__.__mro__:
            if hasattr(base, 'changeListeners'):
                for changeListener in base.changeListeners:
                    changeListener.onCreationEvent(obj)

    @classmethod
    def fireDestructionEvent(cls, obj):
        if hasattr(cls, 'changeListeners'):
            # parcourt les changeListeners de classe
            for changeListener in cls.changeListeners:
                changeListener.onDestructionEvent(obj)
        # parcourt les changeListeners d'instance
        for changeListener in obj.changeListeners:
            changeListener.onDestructionEvent(obj)

    @classmethod
    def fireChangeEvent(cls, obj, field, oldvalue, newvalue):
        if not hasattr(obj, "_changeReady") or not obj._changeReady:
            return
        for base in obj.__class__.__mro__:
            if hasattr(base, 'changeListeners'):
                for changeListener in base.changeListeners:
                    changeListener.onChangeEvent(obj, field, oldvalue, newvalue)
        for changeListener in obj.changeListeners:
            changeListener.onChangeEvent(obj, field, oldvalue, newvalue)


class ModelField(Field):

    def __init__(self, allow_null=False, indexField='id'):
        Field.__init__(self, allow_null)
        self.indexField = indexField

    def to_json(self, instance):
        value = getattr(instance, self.name)
        if value is None:
            return None
        return getattr(value, self.indexField)


class ModelListField(Field):

    def __init__(self, allow_null=False, indexField='id'):
        Field.__init__(self, allow_null)
        self.indexField = indexField

    def to_json(self, instance):
        value_list = getattr(instance, self.name)
        return [getattr(value, self.indexField) for value in value_list]

    def contribute_to_instance(self, instance):
        if not hasattr(instance, self.name):
            setattr(instance, self.name, [])


class ModelDictField(Field):

    def to_json(self, instance):
        value_dict = getattr(instance, self.name)
        return [value.id for value in value_dict.values()]

    def contribute_to_instance(self, instance):
        if not hasattr(instance, self.name):
            setattr(instance, self.name, {})


class ListField(Field):

    def to_json(self, instance):
        try:
            value = getattr(instance, self.name)
            return value[:]
        except Exception:
            return None


class StringField(Field):

    def validate_instance(self, instance):
        super(StringField, self).validate_instance(instance)
        value = getattr(instance, self.name)
        if value is None and self.allow_null:
            return
        if not isinstance(value, basestring):
            raise ValueError("Expected a string value for field %s but got %r." % (self.name, value))


class IntegerField(Field):

    def validate_instance(self, instance):
        super(IntegerField, self).validate_instance(instance)
        value = getattr(instance, self.name)
        if value is None and self.allow_null:
            return
        if not isinstance(value, int):
            raise ValueError("Expected an integer value for field %s but got %r." % (self.name, value))


class BooleanField(Field):

    def validate_instance(self, instance):
        super(BooleanField, self).validate_instance(instance)
        value = getattr(instance, self.name)
        if value is None and self.allow_null:
            return
        if not isinstance(value, bool):
            raise ValueError("%s value must be an integer" % self.name)


class FloatField(Field):

    def validate_instance(self, instance):
        super(FloatField, self).validate_instance(instance)
        value = getattr(instance, self.name)
        if value is None and self.allow_null:
            return
        if not isinstance(coerce(value, 1.0)[0], float):
            raise ValueError("%s value must be a float" % self.name)


class DictField(Field):

    def __init__(self, as_item_list=False, **kwargs):
        Field.__init__(self, **kwargs)
        self.as_item_list = as_item_list

    def to_json(self, instance):
        value_dict = getattr(instance, self.name)
        if self.as_item_list:
            return value_dict.items()
        else:
            return dict(value_dict.items())


class StrategyField(Field):

    def to_json(self, instance):
        value = Field.to_json(self, instance)
        if value is None:
            return None
        return value.getClassName()


def test():

    class Task(Model):
        name = StringField()
        status = IntegerField()
        taskGroup = ModelField(allow_null=True)
        tasks = ModelListField()

        def __init__(self, **kwargs):
            super(Task, self).__init__(**kwargs)

    class MegaTask(Task):
        plop = BooleanField()

        def __init__(self, **kwargs):
            super(MegaTask, self).__init__(**kwargs)

    class ObjectListener(object):

        def __init__(self):
            self.created = False
            self.modified = False

        def onCreationEvent(self, *args):
            self.created = True
            print "created..."

        def onDestructionEvent(self, *args):
            print "deleted"

        def onChangeEvent(self, obj, name, oldvalue, newvalue):
            self.modified = True
            print "modified: %r.%s (%r -> %r)" % (obj, name, oldvalue, newvalue)

    l = ObjectListener()
    Task.changeListeners.append(l)

    t = MegaTask()
    try:
        print t.to_json()
    except ValueError:
        print "ValueError as expected"
    else:
        print "errrr"

    t.id = 7
    t.name = "some task"
    t.status = 42
    t.taskGroup = None
    t.plop = False

    assert t.status == 42
    assert t.taskGroup == None
    assert t.plop == False
    assert t.name == "some task"
    assert t.id == 7

    t.id = 8

    assert l.created
    assert l.modified

#    for i in xrange(8, 17):
#        st = Task(id=i, name="subtask %d" % i, status=42, taskGroup=t)
#        t.tasks.append(st)
#
#    print t.to_json()

if __name__ == '__main__':
    test()
