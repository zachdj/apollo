import json
import logging
from pathlib import Path

import pickle5 as pickle

import apollo


logger = logging.getLogger(__name__)


def list_templates():
    '''List the model templates in the Apollo database.

    Untrained models can be constructed from these template names using
    :func:`apollo.models.from_named_template`.

    Returns:
        list of str:
            The named templates.
    '''
    base = apollo.path('templates')
    base.mkdir(parents=True, exist_ok=True)
    template_paths = base.glob('*.json')
    template_stems = [p.stem for p in template_paths]
    return template_stems


def list_models():
    '''List trained models in the Apollo database.

    Trained models can be constructed from these names using
    :func:`apollo.models.load_named_model`.

    Returns:
        list of str:
            The trained models.
    '''
    base = apollo.path('models')
    base.mkdir(parents=True, exist_ok=True)
    model_paths = base.glob('*.pickle')
    model_stems = [p.stem for p in model_paths]
    return model_stems


def make_estimator(e):
    '''An enhanced version of :func:`sklearn.pipeline.make_pipeline`.

    If the input is a string, it is interpreted as a dotted import path to
    a constructor for the estimator. That constructor is called without
    arguments to create the estimator.

    If the input is a list, it is interpreted as a pipeline of transformers and
    estimators. Each element must be a pair ``(name, params)`` where ``name``
    is a dotted import path to a constructor, and ``params`` is a dict
    providing hyper parameters. The final step must be an estimator, and the
    intermediate steps must be transformers.

    If the input is any other object, it is checked to contain ``fit`` and
    ``predict`` methods and is assumed to be the estimator.

    Otherwise this function raises an :class:`ValueError`.

    Returns:
        sklearn.base.BaseEstimator:
            The estimator.

    Raises:
        ValueError:
            The input could not be cast to an estimator.
    '''
    # If ``e`` is a dotted import path, import it then call it.
    if isinstance(e, str):
        ctor = apollo._import_from_str(e)
        estimator = ctor()

    # If it has a length, interpret ``e`` as a list of pipeline steps.
    elif hasattr(e, '__len__'):
        steps = []
        for (name, params) in e:
            if isinstance(ctor, str):
                ctor = apollo._import_from_str(name)
            step = ctor(**params)
            steps.append(step)
        estimator = make_pipeline(*steps)

    # Otherwise interpret ``e`` directly as an estimator.
    else:
        estimator = e

    # Ensure that it at least has `fit` and `predict`.
    try:
        getattr(estimator, 'fit')
        getattr(estimator, 'predict')
    except AttributeError:
        raise ValueError('could not cast into an estimator')

    return estimator


def load_model(path):
    '''Load a model from a file.

    Arguments:
        path (str or pathlib.Path):
            A path to a model.

    Returns:
        apollo.models.Model:
            The model.
    '''
    path = Path(path)
    fd = path.open('rb')
    model = pickle.load(fd)
    assert isinstance(model, Model), f'{path} is not an Apollo model'
    return model


def load_named_model(name):
    '''Load a model from the Apollo database.

    Models in the Apollo database can be listed with :func:`list_models`
    or from the command line with ``apollo ls models``.

    Arguments:
        name (str):
            The name of the model.

    Returns:
        apollo.models.Model:
            The model.
    '''
    path = apollo.path(f'models/{name}.model')
    return load(path)


def from_template(template, **kwargs):
    '''Construct a model from a template.

    A template is a dictionary giving keyword arguments for the constructor
    :class:`apollo.models.Model`. Alternativly, the dictionary may contain
    the key ``_cls`` giving a dotted import path to an alternate constructor.

    The ``template`` argument may take several forms:

    :class:`dict`
        A dictionary is interpreted as a template directly.
    file-like object
        A file-like object is parsed as JSON.
    :class:`pathlib.Path` or :class:`str`
        A path to a JSON file containing the template.

    Arguments:
        template (dict or str or pathlib.Path or io.IOBase):
            A template dictionary or path to a template file.
        **kwargs:
            Additional keyword arguments to pass to the model constructor.

    Returns:
        apollo.models.Model:
            An untrained model.
    '''
    # Convert str to Path.
    if isinstance(template, str):
        template = Path(template)

    # Convert Path to file-like.
    if isinstance(template, Path):
        template = template.open('r')

    # Convert file-like to dict.
    if hasattr(template, 'read'):
        template = json.load(template)

    # The kwargs override the template.
    template.update(kwargs)

    # Determine which class to instantiate.
    cls = template.pop('_cls', 'apollo.models.NamModel')
    cls = apollo._import_from_str(cls)

    # Load from dict.
    logger.debug(f'using template: {template}')
    model = cls(**template)
    return model


def from_named_template(template_name, **kwargs):
    '''Load a model from named template in the Apollo database.

    Templates in the Apollo database can be listed with :func:`list_templates`
    or from the command line with ``apollo ls templates``.

    Arguments:
        template_name (str):
            The name of a template in the Apollo database.
        **kwargs:
            Additional keyword arguments to pass to the model constructor.

    Returns:
        apollo.models.Model:
            An untrained model.
    '''
    template = apollo.path(f'templates/{template_name}.json')
    return from_template(template)
