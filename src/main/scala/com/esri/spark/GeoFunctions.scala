package com.esri.spark

import org.apache.spark.sql.Column

object GeoFunctions {
  def stManhattan(c1: Column,
                  c2: Column,
                  c3: Column,
                  c4: Column
                 ): Column =
    new Column(STManhattan(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stPoint(c1: Column,
              c2: Column
             ): Column =
    new Column(STPoint(Array(c1.expr, c2.expr)))

  def stLine(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column
            ): Column =
    new Column(STLine(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stRect(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column
            ): Column =
    new Column(STRect(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stCell(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column
            ): Column =
    new Column(STCell(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stBox(c1: Column,
            c2: Column,
            c3: Column,
            c4: Column
           ): Column =
    new Column(STBox(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stAsText(c1: Column): Column =
    new Column(STAsText(Array(c1.expr)))

  def stAsGeoJSON(c1: Column): Column =
    new Column(STAsGeoJSON(Array(c1.expr)))

  def stFromText(c1: Column): Column =
    new Column(STFromText(Array(c1.expr)))

  def stLonToX(c1: Column): Column =
    new Column(STLonToX(Array(c1.expr)))

  def stLatToY(c1: Column): Column =
    new Column(STLatToY(Array(c1.expr)))

  def stLonToQ(c1: Column,
               c2: Column
              ): Column =
    new Column(STLonToQ(Array(c1.expr, c2.expr)))

  def stLatToR(c1: Column,
               c2: Column
              ): Column =
    new Column(STLatToR(Array(c1.expr, c2.expr)))

  def stQToX(c1: Column,
             c2: Column,
             c3: Column
            ): Column =
    new Column(STQToX(Array(c1.expr, c2.expr, c3.expr)))

  def stXToLon(c1: Column): Column =
    new Column(STXToLon(Array(c1.expr)))

  def stYToLat(c1: Column): Column =
    new Column(STYToLat(Array(c1.expr)))

  def stPolyline(c1: Column): Column =
    new Column(STPolyline(Array(c1.expr)))

  def stPolygon(c1: Column): Column =
    new Column(STPolygon(Array(c1.expr)))

  def stPolyline2(c1: Column): Column =
    new Column(STPolyline2(Array(c1.expr)))

  def stPolygon2(c1: Column): Column =
    new Column(STPolygon2(Array(c1.expr)))

  def stIntersection(c1: Column,
                     c2: Column,
                     c3: Column,
                    ): Column =
    new Column(STIntersection(Array(c1.expr, c2.expr, c3.expr)))

  def stIntersects(c1: Column,
                   c2: Column,
                   c3: Column,
                  ): Column =
    new Column(STIntersects(Array(c1.expr, c2.expr, c3.expr)))

  def stIntersectsBox(c1: Column,
                      c2: Column,
                      c3: Column,
                      c4: Column,
                      c5: Column,
                     ): Column =
    new Column(STIntersectsBox(Array(c1.expr, c2.expr, c3.expr, c4.expr, c5.expr)))

  def stContains(c1: Column,
                 c2: Column,
                 c3: Column,
                ): Column =
    new Column(STContains(Array(c1.expr, c2.expr, c3.expr)))

  def stWithin(c1: Column,
               c2: Column,
               c3: Column,
              ): Column =
    new Column(STWithin(Array(c1.expr, c2.expr, c3.expr)))

  def stDisjoint(c1: Column,
                 c2: Column,
                 c3: Column,
                ): Column =
    new Column(STDisjoint(Array(c1.expr, c2.expr, c3.expr)))

  def stOverlaps(c1: Column,
                 c2: Column,
                 c3: Column,
                ): Column =
    new Column(STOverlaps(Array(c1.expr, c2.expr, c3.expr)))

  def stTouches(c1: Column,
                c2: Column,
                c3: Column,
               ): Column =
    new Column(STTouches(Array(c1.expr, c2.expr, c3.expr)))

  def stIsEmpty(c1: Column): Column =
    new Column(STIsEmpty(Array(c1.expr)))

  def stDistance(c1: Column,
                 c2: Column
                ): Column =
    new Column(STDistance(Array(c1.expr, c2.expr)))

  def qrEnvp(c1: Column,
             c2: Column,
             c3: Column
            ): Column =
    new Column(QREnvp(Array(c1.expr, c2.expr, c3.expr)))

  def qrClip(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column,
            ): Column =
    new Column(QRClip(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def qrContainsGeom(c1: Column,
                     c2: Column,
                     c3: Column
                    ): Column =
    new Column(QRContainsGeom(Array(c1.expr, c2.expr, c3.expr)))

  def qrList(c1: Column,
             c2: Column,
             c3: Column
            ): Column =
    new Column(QRList(Array(c1.expr, c2.expr, c3.expr)))

  def qrIntersect(c1: Column,
                  c2: Column,
                  c3: Column
                 ): Column =
    new Column(QRIntersect(Array(c1.expr, c2.expr, c3.expr)))

  def qrFromXY(c1: Column,
               c2: Column,
               c3: Column
              ): Column =
    new Column(QRFromXY(Array(c1.expr, c2.expr, c3.expr)))

  def qrFromGeom(c1: Column,
                 c2: Column,
                ): Column =
    new Column(QRFromGeom(Array(c1.expr, c2.expr)))

  def stX(c1: Column,
          c2: Column
         ): Column =
    new Column(STX(Array(c1.expr, c2.expr)))

  def stY(c1: Column,
          c2: Column
         ): Column =
    new Column(STY(Array(c1.expr, c2.expr)))

  def stEuclid(c1: Column,
               c2: Column,
               c3: Column,
               c4: Column
              ): Column =
    new Column(STEuclid(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stHaversine(c1: Column,
                  c2: Column,
                  c3: Column,
                  c4: Column
                 ): Column =
    new Column(STHaversine(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stXToQ(c1: Column,
             c2: Column
            ): Column =
    new Column(STXToQ(Array(c1.expr, c2.expr)))

  def stXY(c1: Column,
           c2: Column
          ): Column =
    new Column(STXY(Array(c1.expr, c2.expr)))

  def stCentroid(c1: Column): Column =
    new Column(STCentroid(Array(c1.expr)))

  def stCentroidXY(c1: Column): Column =
    new Column(STCentroidXY(Array(c1.expr)))

  def stBuffer(c1: Column,
               c2: Column,
               c3: Column,
               c4: Column,
              ): Column =
    new Column(STBuffer(Array(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stConvexHull(c1: Column): Column =
    new Column(STConvexHull(Array(c1.expr)))

  def stMercator(c1: Column): Column =
    new Column(STMercator(Array(c1.expr)))

  def stWGS84(c1: Column): Column =
    new Column(STWGS84(Array(c1.expr)))

  def stMultipoint(c1: Column): Column =
    new Column(STMultipoint(Array(c1.expr)))

  def stUnionCol(c1: Column,
                 c2: Column
                ): Column =
    new Column(STUnionCol(Array(c1.expr, c2.expr)))

  def stExteriorRing(c1: Column): Column =
    new Column(STExteriorRing(Array(c1.expr)))

  def stIoU(c1: Column,
            c2: Column,
            c3: Column,
           ): Column =
    new Column(STIoU(Array(c1.expr, c2.expr, c3.expr)))

  def stExtent(c1: Column): Column =
    new Column(STExtent(Array(c1.expr)))

  def stSimplify(c1: Column,
                 c2: Column
                ): Column =
    new Column(STSimplify(Array(c1.expr, c2.expr)))

  def stRepair(c1: Column,
               c2: Column
              ): Column =
    new Column(STRepair(Array(c1.expr, c2.expr)))

  def stDump(c1: Column): Column =
    new Column(STDump(Array(c1.expr)))

  def gdbPolygon2(c1: Column): Column =
    new Column(GDBPolygon2(Array(c1.expr)))

  def gdbPolygonM(c1: Column): Column =
    new Column(GDBPolygonM(Array(c1.expr)))

  def gdbPolygonZ(c1: Column): Column =
    new Column(GDBPolygonZ(Array(c1.expr)))

  def gdbPolyline2(c1: Column): Column =
    new Column(GDBPolyline2(Array(c1.expr)))

  def stArea(c1: Column): Column =
    new Column(STArea(Array(c1.expr)))

  def stLength(c1: Column): Column =
    new Column(STLength(Array(c1.expr)))

  def stTranslate(c1: Column,
                  c2: Column,
                  c3: Column
                 ): Column =
    new Column(STTranslate(Array(c1.expr, c2.expr, c3.expr)))

  def h3LatLngToCell(c1: Column,
                     c2: Column,
                     c3: Column
                    ): Column =
    new Column(H3LatLngToCell(Array(c1.expr, c2.expr, c3.expr)))

  def h3CellToBoundary(c1: Column): Column =
    new Column(H3CellToBoundary(Array(c1.expr)))
}
