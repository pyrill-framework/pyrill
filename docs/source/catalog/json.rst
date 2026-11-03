====
JSON
====

Usually, :class:`~pyrill.json.ToJson` is the only encoding stage you need. It
handles scalars, strings, mappings and collections, including nested values,
and automatically chooses the object or array serializer. Its output arrives
in text fragments; join them to obtain the complete JSON document or pass them
directly to a text sink.

Pass one complete value per input frame. For example, a frame containing a list
becomes a JSON array. Successive frames produce successive documents without
separators, so use :class:`~pyrill.json.ToJsonPerLine` when you need JSON Lines.

Use :class:`~pyrill.json.ToJsonList` when separate input frames should become
items of one array, or :class:`~pyrill.json.ToJsonObject` when a stream of
key-value pairs should become one object. The inner serializers are building
blocks for these stages and are rarely needed directly.

:class:`~pyrill.json.FromJson` decodes one complete JSON document per input frame
into a Python value.

.. automodule:: pyrill.json
