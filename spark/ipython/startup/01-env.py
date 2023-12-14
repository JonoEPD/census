from IPython import get_ipython

sys.path.append("/home/iceberg/app")
get_ipython().run_line_magic("load_ext", "autoreload")
get_ipython().run_line_magic("autoreload", "2")