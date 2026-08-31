package com.esri.spark

import org.apache.spark.sql.esri.ColumnCompat._
import org.apache.spark.sql.Column

object GeoFunctions {
  def stManhattan(c1: Column,
                  c2: Column,
                  c3: Column,
                  c4: Column
                 ): Column =
    toColumn(STManhattan(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stPoint(c1: Column,
              c2: Column
             ): Column =
    toColumn(STPoint(Seq(c1.expr, c2.expr)))

  def stLine(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column
            ): Column =
    toColumn(STLine(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stRect(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column
            ): Column =
    toColumn(STRect(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stCell(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column
            ): Column =
    toColumn(STCell(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stBox(c1: Column,
            c2: Column,
            c3: Column,
            c4: Column
           ): Column =
    toColumn(STBox(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stAsText(c1: Column): Column =
    toColumn(STAsText(Seq(c1.expr)))

  def stAsGeoJSON(c1: Column): Column =
    toColumn(STAsGeoJSON(Seq(c1.expr)))

  def stFromText(c1: Column): Column =
    toColumn(STFromText(Seq(c1.expr)))

  def stLonToX(c1: Column): Column =
    toColumn(STLonToX(Seq(c1.expr)))

  def stLatToY(c1: Column): Column =
    toColumn(STLatToY(Seq(c1.expr)))

  def stLonToQ(c1: Column,
               c2: Column
              ): Column =
    toColumn(STLonToQ(Seq(c1.expr, c2.expr)))

  def stLatToR(c1: Column,
               c2: Column
              ): Column =
    toColumn(STLatToR(Seq(c1.expr, c2.expr)))

  def stQToX(c1: Column,
             c2: Column,
             c3: Column
            ): Column =
    toColumn(STQToX(Seq(c1.expr, c2.expr, c3.expr)))

  def stXToLon(c1: Column): Column =
    toColumn(STXToLon(Seq(c1.expr)))

  def stYToLat(c1: Column): Column =
    toColumn(STYToLat(Seq(c1.expr)))

  def stPolyline(c1: Column): Column =
    toColumn(STPolyline(Seq(c1.expr)))

  def stPolygon(c1: Column): Column =
    toColumn(STPolygon(Seq(c1.expr)))

  def stPolyline2(c1: Column): Column =
    toColumn(STPolyline2(Seq(c1.expr)))

  def stPolygon2(c1: Column): Column =
    toColumn(STPolygon2(Seq(c1.expr)))

  def stIntersection(c1: Column,
                     c2: Column,
                     c3: Column,
                    ): Column =
    toColumn(STIntersection(Seq(c1.expr, c2.expr, c3.expr)))

  def stIntersects(c1: Column,
                   c2: Column,
                   c3: Column,
                  ): Column =
    toColumn(STIntersects(Seq(c1.expr, c2.expr, c3.expr)))

  def stIntersectsBox(c1: Column,
                      c2: Column,
                      c3: Column,
                      c4: Column,
                      c5: Column,
                     ): Column =
    toColumn(STIntersectsBox(Seq(c1.expr, c2.expr, c3.expr, c4.expr, c5.expr)))

  def stContains(c1: Column,
                 c2: Column,
                 c3: Column,
                ): Column =
    toColumn(STContains(Seq(c1.expr, c2.expr, c3.expr)))

  def stWithin(c1: Column,
               c2: Column,
               c3: Column,
              ): Column =
    toColumn(STWithin(Seq(c1.expr, c2.expr, c3.expr)))

  def stDisjoint(c1: Column,
                 c2: Column,
                 c3: Column,
                ): Column =
    toColumn(STDisjoint(Seq(c1.expr, c2.expr, c3.expr)))

  def stOverlaps(c1: Column,
                 c2: Column,
                 c3: Column,
                ): Column =
    toColumn(STOverlaps(Seq(c1.expr, c2.expr, c3.expr)))

  def stTouches(c1: Column,
                c2: Column,
                c3: Column,
               ): Column =
    toColumn(STTouches(Seq(c1.expr, c2.expr, c3.expr)))

  def stIsEmpty(c1: Column): Column =
    toColumn(STIsEmpty(Seq(c1.expr)))

  def stDistance(c1: Column,
                 c2: Column
                ): Column =
    toColumn(STDistance(Seq(c1.expr, c2.expr)))

  def qrEnvp(c1: Column,
             c2: Column,
             c3: Column
            ): Column =
    toColumn(QREnvp(Seq(c1.expr, c2.expr, c3.expr)))

  def qrEnvpGeom(c1: Column,
                 c2: Column,
                 c3: Column,
                 c4: Column,
                ): Column =
    toColumn(QREnvpGeom(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def qrGeom(c1: Column,
             c2: Column,
             c3: Column,
             c4: Column,
            ): Column =
    toColumn(QRGeom(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def qrContainsGeom(c1: Column,
                     c2: Column,
                     c3: Column
                    ): Column =
    toColumn(QRContainsGeom(Seq(c1.expr, c2.expr, c3.expr)))

  def qrCount(c1: Column,
              c2: Column,
              c3: Column
             ): Column =
    toColumn(QRCount(Seq(c1.expr, c2.expr, c3.expr)))

  def qrList(c1: Column,
             c2: Column,
             c3: Column
            ): Column =
    toColumn(QRList(Seq(c1.expr, c2.expr, c3.expr)))

  def qrIntersect(c1: Column,
                  c2: Column,
                  c3: Column
                 ): Column =
    toColumn(QRIntersect(Seq(c1.expr, c2.expr, c3.expr)))

  def qrFromXY(c1: Column,
               c2: Column,
               c3: Column
              ): Column =
    toColumn(QRFromXY(Seq(c1.expr, c2.expr, c3.expr)))

  def qrFromGeom(c1: Column,
                 c2: Column,
                ): Column =
    toColumn(QRFromGeom(Seq(c1.expr, c2.expr)))

  def qrAsGeom(c1: Column,
               c2: Column,
               c3: Column,
              ): Column =
    toColumn(QRAsGeom(Seq(c1.expr, c2.expr, c3.expr)))

  def stX(c1: Column,
          c2: Column
         ): Column =
    toColumn(STX(Seq(c1.expr, c2.expr)))

  def stY(c1: Column,
          c2: Column
         ): Column =
    toColumn(STY(Seq(c1.expr, c2.expr)))

  def stEuclid(c1: Column,
               c2: Column,
               c3: Column,
               c4: Column
              ): Column =
    toColumn(STEuclid(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stHaversine(c1: Column,
                  c2: Column,
                  c3: Column,
                  c4: Column
                 ): Column =
    toColumn(STHaversine(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stXToQ(c1: Column,
             c2: Column
            ): Column =
    toColumn(STXToQ(Seq(c1.expr, c2.expr)))

  def stXY(c1: Column,
           c2: Column
          ): Column =
    toColumn(STXY(Seq(c1.expr, c2.expr)))

  def stCentroid(c1: Column): Column =
    toColumn(STCentroid(Seq(c1.expr)))

  def stCentroidXY(c1: Column): Column =
    toColumn(STCentroidXY(Seq(c1.expr)))

  def stBuffer(c1: Column,
               c2: Column,
               c3: Column,
               c4: Column,
              ): Column =
    toColumn(STBuffer(Seq(c1.expr, c2.expr, c3.expr, c4.expr)))

  def stConvexHull(c1: Column): Column =
    toColumn(STConvexHull(Seq(c1.expr)))

  def stMercator(c1: Column): Column =
    toColumn(STMercator(Seq(c1.expr)))

  def stWGS84(c1: Column): Column =
    toColumn(STWGS84(Seq(c1.expr)))

  def stMultipoint(c1: Column): Column =
    toColumn(STMultipoint(Seq(c1.expr)))

  def stUnionCol(c1: Column,
                 c2: Column
                ): Column =
    toColumn(STUnionCol(Seq(c1.expr, c2.expr)))

  def stExteriorRing(c1: Column): Column =
    toColumn(STExteriorRing(Seq(c1.expr)))

  def stIoU(c1: Column,
            c2: Column,
            c3: Column,
           ): Column =
    toColumn(STIoU(Seq(c1.expr, c2.expr, c3.expr)))

  def stExtent(c1: Column): Column =
    toColumn(STExtent(Seq(c1.expr)))

  def stSimplify(c1: Column,
                 c2: Column
                ): Column =
    toColumn(STSimplify(Seq(c1.expr, c2.expr)))

  def stRepair(c1: Column,
               c2: Column
              ): Column =
    toColumn(STRepair(Seq(c1.expr, c2.expr)))

  def stDump(c1: Column): Column =
    toColumn(STDump(Seq(c1.expr)))

  def gdbPolygon2(c1: Column): Column =
    toColumn(GDBPolygon2(Seq(c1.expr)))

  def gdbPolygonM(c1: Column): Column =
    toColumn(GDBPolygonM(Seq(c1.expr)))

  def gdbPolygonZ(c1: Column): Column =
    toColumn(GDBPolygonZ(Seq(c1.expr)))

  def gdbPolyline2(c1: Column): Column =
    toColumn(GDBPolyline2(Seq(c1.expr)))

  def stArea(c1: Column): Column =
    toColumn(STArea(Seq(c1.expr)))

  def stLength(c1: Column): Column =
    toColumn(STLength(Seq(c1.expr)))

  def stTranslate(c1: Column,
                  c2: Column,
                  c3: Column
                 ): Column =
    toColumn(STTranslate(Seq(c1.expr, c2.expr, c3.expr)))

  def h3LatLngToCell(c1: Column,
                     c2: Column,
                     c3: Column
                    ): Column =
    toColumn(H3LatLngToCell(Seq(c1.expr, c2.expr, c3.expr)))

  def h3CellToBoundary(c1: Column): Column =
    toColumn(H3CellToBoundary(Seq(c1.expr)))

  def clipLine(c1: Column,
               c2: Column
              ): Column =
    toColumn(ClipLine(c1.expr, c2.expr))

  def clipLineDist(c1: Column,
                   c2: Column,
                   c3: Column
                  ): Column =
    toColumn(ClipLineDist(c1.expr, c2.expr, c3.expr))
}
