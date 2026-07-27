#
# pip install pyperclip
#
import arcpy
import os
import re
import tempfile
from math import floor

try:
    import pyperclip
except ImportError:
    pyperclip = None

class Toolbox(object):
    def __init__(self):
        self.label = "ExtentToolbox"
        self.alias = "ExtentToolbox"
        self.tools = [ExtentTool, QRTool, XYMinMax, XYTool]


class ExtentTool(object):
    def __init__(self):
        self.label = "ExtentTool"
        self.description = "ExtentTool"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
            name="param_fc",
            displayName="outputFC",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        param_bb = arcpy.Parameter(
            name="param_bb",
            displayName="Bounding Box",
            direction="Input",
            datatype="GPExtent",
            parameterType="Required")

        param_sp_ref = arcpy.Parameter(
            name="param_sp_ref",
            displayName="Output Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        param_sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        param_ofs = arcpy.Parameter(
            name="param_ofs",
            displayName="Offset in Output Units",
            direction="Input",
            datatype="GPDouble",
            parameterType="Required")
        param_ofs.value = 0.0

        param_name = arcpy.Parameter(
            name="param_name",
            displayName="Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_name.value = "Extent"

        param_memory = arcpy.Parameter(
            name="in_memory",
            displayName="Use Memory Workspace",
            direction="Input",
            datatype="Boolean",
            parameterType="Optional")
        param_memory.value = False

        return [param_fc, param_bb, param_sp_ref, param_ofs, param_name, param_memory]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, _):
        ext = parameters[1].value
        sp_ref = parameters[2].value
        ofs = parameters[3].value
        name = parameters[4].value
        ws = "memory" if parameters[5].value else arcpy.env.scratchGDB

        last_symbology = None
        project = arcpy.mp.ArcGISProject('current')
        for layer in project.activeMap.listLayers():
            if layer.name == name:
                last_symbology = os.path.join(tempfile.mkdtemp(), name)
                layer.saveACopy(last_symbology)
                arcpy.AddMessage(last_symbology)

        geo = ext.projectAs(sp_ref.exportToString())
        x_min = geo.XMin - ofs
        y_min = geo.YMin - ofs
        x_max = geo.XMax + ofs
        y_max = geo.YMax + ofs

        fc = os.path.join(ws, name)

        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        arcpy.management.CreateFeatureclass(ws, name, "POLYGON", spatial_reference=sp_ref)
        arcpy.management.AddField(fc, "EXTENT_LEFT", "DOUBLE")
        arcpy.management.AddField(fc, "EXTENT_RIGHT", "DOUBLE")
        arcpy.management.AddField(fc, "EXTENT_TOP", "DOUBLE")
        arcpy.management.AddField(fc, "EXTENT_BOTTOM", "DOUBLE")
        arcpy.management.AddField(fc, "CENTER_X", "DOUBLE")
        arcpy.management.AddField(fc, "CENTER_Y", "DOUBLE")
        arcpy.management.AddField(fc, "EXTENT", "TEXT", field_length=128)
        fields = ["SHAPE@",
                  "EXTENT_LEFT", "EXTENT_RIGHT",
                  "EXTENT_TOP", "EXTENT_BOTTOM",
                  "CENTER_X", "CENTER_Y",
                  "EXTENT"]
        with arcpy.da.InsertCursor(fc, fields) as cursor:
            shape = [[x_min, y_min],
                     [x_max, y_min],
                     [x_max, y_max],
                     [x_min, y_max],
                     [x_min, y_min]]
            text = f"xmin,ymin,xmax,ymax = ({x_min},{y_min},{x_max},{y_max})"
            if pyperclip is not None:
                pyperclip.copy(text)
            cursor.insertRow([shape,
                              x_min, x_max,
                              y_max, y_min,
                              (x_min + x_max) * 0.5, (y_min + y_max) * 0.5,
                              text])
        if last_symbology:
            parameters[0].symbology = f"{last_symbology}.lyrx"
        else:
            symbology = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"{name}.lyrx")
            if os.path.exists(symbology):
                parameters[0].symbology = symbology
        parameters[0].value = fc


class QRTool(object):
    def __init__(self):
        self.label = "QRTool"
        self.description = "QRTool"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
            name="param_fc",
            displayName="outputFC",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        param_bb = arcpy.Parameter(
            name="param_bb",
            displayName="Bounding Box",
            direction="Input",
            datatype="GPExtent",
            parameterType="Required")

        param_sp_ref = arcpy.Parameter(
            name="param_sp_ref",
            displayName="Output Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        param_sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        param_cell = arcpy.Parameter(
            name="param_cell",
            displayName="Cell Size",
            direction="Input",
            datatype="GPDouble",
            parameterType="Required")
        param_cell.value = 1.0

        param_name = arcpy.Parameter(
            name="param_name",
            displayName="Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_name.value = "QR"

        param_memory = arcpy.Parameter(
            name="in_memory",
            displayName="Use Memory Workspace",
            direction="Input",
            datatype="Boolean",
            parameterType="Optional")
        param_memory.value = False

        return [param_fc, param_bb, param_sp_ref, param_cell, param_name, param_memory]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, _):
        ext = parameters[1].value
        sp_ref = parameters[2].value
        cell = parameters[3].value
        name = parameters[4].value
        ws = "memory" if parameters[5].value else arcpy.env.scratchGDB

        last_symbology = None
        project = arcpy.mp.ArcGISProject('current')
        for layer in project.activeMap.listLayers():
            if layer.name == name:
                last_symbology = os.path.join(tempfile.mkdtemp(), name)
                layer.saveACopy(last_symbology)
                arcpy.AddMessage(last_symbology)

        geo = ext.projectAs(sp_ref.exportToString())
        q_min = floor(geo.XMin / cell)
        r_min = floor(geo.YMin / cell)
        q_max = floor(geo.XMax / cell) + 1
        r_max = floor(geo.YMax / cell) + 1
        arcpy.AddMessage(f"q:{q_min} -> {q_max}, r:{r_min} -> {r_max}")

        fc = os.path.join(ws, name)
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

        arcpy.env.autoCancelling = False
        arcpy.management.CreateFeatureclass(ws, name, "POLYGON", spatial_reference=sp_ref)
        arcpy.management.AddField(fc, "NAME", "TEXT")
        arcpy.management.AddField(fc, "Q", "LONG")
        arcpy.management.AddField(fc, "R", "LONG")
        fields = ["SHAPE@", "NAME", "Q", "R"]
        with arcpy.da.InsertCursor(fc, fields) as cursor:
            not_cancelled = not arcpy.env.isCancelled
            q = q_min
            while not_cancelled and q < q_max:
                q32 = (q & 0xFFFFFFFF) << 32
                x_min = q * cell
                x_max = x_min + cell
                r = r_min
                while not_cancelled and r < r_max:
                    y_min = r * cell
                    y_max = y_min + cell
                    shape = [[x_min, y_min],
                             [x_max, y_min],
                             [x_max, y_max],
                             [x_min, y_max],
                             [x_min, y_min]]

                    qr = q32 | (r & 0xFFFFFFFF)
                    cursor.insertRow([shape, f"{qr:016X}", q, r])
                    not_cancelled = not arcpy.env.isCancelled
                    r += 1
                q += 1
        if last_symbology:
            parameters[0].symbology = f"{last_symbology}.lyrx"
        parameters[0].value = fc


class XYMinMax(object):
    def __init__(self):
        self.label = "XYMinMax"
        self.description = "Create polygon extent from 'xmin ymin xmax ymax'"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
            name="param_fc",
            displayName="outputFC",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        param_bb = arcpy.Parameter(
            name="param_bb",
            displayName="Xmin Ymin Xmax Ymax",
            direction="Input",
            datatype="GPString",
            parameterType="Required")

        param_sp_ref = arcpy.Parameter(
            name="param_sp_ref",
            displayName="Input Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        param_sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        param_name = arcpy.Parameter(
            name="param_name",
            displayName="Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_name.value = "Extent"

        param_memory = arcpy.Parameter(
            name="in_memory",
            displayName="Use Memory Workspace",
            direction="Input",
            datatype="Boolean",
            parameterType="Optional")
        param_memory.value = True

        return [param_fc, param_bb, param_sp_ref, param_name, param_memory]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, _):
        xy_min_max = parameters[1].value
        sp_ref = parameters[2].value
        name = parameters[3].value
        ws = "memory" if parameters[4].value else arcpy.env.scratchGDB
        fc = os.path.join(ws, name)
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)
        arr = re.split("[ ,;]", xy_min_max)
        if len(arr) != 4:
            arcpy.AddError("Expecting xmin ymin xmax ymax")
            return
        x_min, y_min, x_max, y_max = arr
        arcpy.management.CreateFeatureclass(ws, name, "POLYGON", spatial_reference=sp_ref)
        arcpy.management.AddField(fc, "EXTENT_LEFT", "DOUBLE")
        arcpy.management.AddField(fc, "EXTENT_RIGHT", "DOUBLE")
        arcpy.management.AddField(fc, "EXTENT_TOP", "DOUBLE")
        arcpy.management.AddField(fc, "EXTENT_BOTTOM", "DOUBLE")
        fields = ["SHAPE@", "EXTENT_LEFT", "EXTENT_RIGHT", "EXTENT_TOP", "EXTENT_BOTTOM"]
        with arcpy.da.InsertCursor(fc, fields) as cursor:
            shape = [[x_min, y_min],
                     [x_max, y_min],
                     [x_max, y_max],
                     [x_min, y_max],
                     [x_min, y_min]]
            cursor.insertRow([shape, x_min, x_max, y_max, y_min])
        symbology = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"{name}.lyrx")
        if os.path.exists(symbology):
            parameters[0].symbology = symbology
        parameters[0].value = fc


class XYTool(object):
    def __init__(self):
        self.label = "XYTool"
        self.description = "Create point feature class from 'x y'"
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_fc = arcpy.Parameter(
            name="param_fc",
            displayName="outputFC",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        param_xy = arcpy.Parameter(
            name="param_xy",
            displayName="X Y",
            direction="Input",
            datatype="GPString",
            parameterType="Required")

        param_sp_ref = arcpy.Parameter(
            name="param_sp_ref",
            displayName="Input Spatial Reference",
            direction="Input",
            datatype="GPSpatialReference",
            parameterType="Required")
        param_sp_ref.value = arcpy.SpatialReference(4326).exportToString()

        param_name = arcpy.Parameter(
            name="param_name",
            displayName="Name",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        param_name.value = "Point"

        param_label = arcpy.Parameter(
            name="param_label",
            displayName="Label",
            direction="Input",
            datatype="GPString",
            parameterType="Optional")
        param_label.value = ""

        param_memory = arcpy.Parameter(
            name="in_memory",
            displayName="Use Memory Workspace",
            direction="Input",
            datatype="Boolean",
            parameterType="Optional")
        param_memory.value = False

        return [param_fc, param_xy, param_sp_ref, param_name, param_label, param_memory]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, _):
        xy = parameters[1].value
        sp_ref = parameters[2].value
        name = parameters[3].value
        label = parameters[4].value
        ws = "memory" if parameters[-1].value else arcpy.env.scratchGDB

        last_symbology = None
        project = arcpy.mp.ArcGISProject('current')
        for layer in project.activeMap.listLayers():
            if layer.name == name:
                last_symbology = os.path.join(tempfile.mkdtemp(), name)
                layer.saveACopy(last_symbology)
                arcpy.AddMessage(last_symbology)

        fc = os.path.join(ws, name)
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)
        pattern = re.compile(r"""([^\s,]+)[\s,]+(.+)""")
        match = pattern.match(xy)
        if match:
            x = float(match.group(1))
            y = float(match.group(2))
            arcpy.management.CreateFeatureclass(ws, name, "POINT", spatial_reference=sp_ref)
            arcpy.management.AddField(fc, "X", "DOUBLE")
            arcpy.management.AddField(fc, "Y", "DOUBLE")
            arcpy.management.AddField(fc, "LABEL", "TEXT")
            fields = ["SHAPE@X", "SHAPE@Y", "X", "Y", "LABEL"]
            with arcpy.da.InsertCursor(fc, fields) as cursor:
                cursor.insertRow((x, y, x, y, label))

            if last_symbology:
                parameters[0].symbology = f"{last_symbology}.lyrx"
            parameters[0].value = fc
        else:
            arcpy.AddError("Expecting 'x y' or 'x,y")
