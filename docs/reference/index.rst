API Reference
-------------

The API for ``petrelpy`` is split by format between Petrel file formats such as
``ev``, ``vol``, and ``prn``, the ``gslib`` format, Eclipse's ``wcf`` format, and the CLI section.

Petrelpy by file format
=======================
.. toctree::
   :maxdepth: 1

   ../autoapi/petrelpy/petrel/index
   ../autoapi/petrelpy/gslib/index
   ../autoapi/petrelpy/wellconnection/index

Getting data into Petrel
========================
.. autoapisummary::

   petrelpy.petrel.export_vol
   petrelpy.petrel.export_injection_vol
   petrelpy.petrel.export_perfs_ev
   petrelpy.petrel.export_perfs_prn
   petrelpy.petrel.write_header
   petrelpy.petrel.read_header
   petrelpy.petrel.write_tops
   petrelpy.petrel.collect_perfs
   petrelpy.petrel.read_production
   petrelpy.petrel.get_raw_table

Using Petrel exports
====================
.. autoapisummary::

   petrelpy.petrel.convert_properties_petrel_to_arc
   petrelpy.petrel.read_petrel_tops

   petrelpy.gslib.load_from_petrel
   petrelpy.gslib.get_midpoint_cell_columns
   petrelpy.gslib.load_petrel_tops_file
   petrelpy.gslib.match_well_to_cell
   petrelpy.gslib.match_ijz_petrel
   petrelpy.gslib.aggregate_well_properties
   petrelpy.gslib.get_facies_stats
   petrelpy.gslib.get_facies_histograms

   petrelpy.wellconnection.process_well_connection_file
   petrelpy.wellconnection.process_well_lateral
   petrelpy.wellconnection.get_wellnames
   petrelpy.wellconnection.get_trajectory_geomodel_columns
   petrelpy.wellconnection.get_trajectory
   petrelpy.wellconnection.get_well
   petrelpy.wellconnection.get_wellname

Command Line Interface
======================

Some documentation for the CLI tools are here
(but you're better off checking `Getting started <../getting-started/>`__):

.. toctree::
   :maxdepth: 1

   ../autoapi/petrelpy/cli/index
