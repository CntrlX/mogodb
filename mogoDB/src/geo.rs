//! Geospatial predicates (GeoJSON + legacy coordinate pairs), pure Rust.
//!
//! Coordinates are `(lng, lat)` for spherical data and plain `(x, y)` for planar legacy data.
//! Spherical distances use the haversine formula with [`EARTH_RADIUS_M`]. Polygon predicates
//! (`$geoWithin` with `$geometry`/`$polygon`/`$box`/`$center`, `$geoIntersects`) use planar
//! point-in-polygon and segment tests on the raw coordinates, as an approximation of MongoDB's
//! spherical semantics.
use crate::bsonutil::num_f64;
use crate::error::{Error, Result};
use bson::{Bson, Document};

/// Mean earth radius in meters as used by MongoDB.
pub const EARTH_RADIUS_M: f64 = 6378100.0;

/// Tolerance for "point lies on segment" tests (in coordinate units).
const EPS: f64 = 1e-9;

type P = (f64, f64);

#[derive(Clone, Debug, PartialEq)]
pub enum Geometry {
    Point((f64, f64)),
    MultiPoint(Vec<(f64, f64)>),
    LineString(Vec<(f64, f64)>),
    MultiLineString(Vec<Vec<(f64, f64)>>),
    /// Outer ring first, then holes. Rings are closed (first == last).
    Polygon(Vec<Vec<(f64, f64)>>),
    MultiPolygon(Vec<Vec<Vec<(f64, f64)>>>),
    GeometryCollection(Vec<Geometry>),
}

/// Parse a GeoJSON object, legacy `[x, y]` array, or `{x: .., y: ..}` / any 2-number document.
pub fn parse_geometry(v: &Bson) -> Option<Geometry> {
    match v {
        Bson::Document(d) => {
            if d.get_str("type").is_ok() {
                parse_geojson(d)
            } else {
                legacy_doc_point(d, false).map(Geometry::Point)
            }
        }
        Bson::Array(_) => pos(v).map(Geometry::Point),
        _ => None,
    }
}

/// Why a value cannot produce 2dsphere keys (MongoDB's underlying reason), with the
/// `failingElement` to report. `Ok` for values that index nothing (null, `{}`) or are valid.
pub fn check_2dsphere(v: &Bson) -> std::result::Result<(), (String, Bson)> {
    let shell = crate::cmd::admin::shell_repr;
    let in_bounds = |(x, y): P| (-180.0..=180.0).contains(&x) && (-90.0..=90.0).contains(&y);
    let flat = |v: &Bson| ("can't project geometry from FLAT CRS into spherical CRS".to_string(), Bson::Document(bson::doc! {"element": v.clone()}));
    match v {
        Bson::Null | Bson::Undefined => Ok(()),
        Bson::Array(a) => {
            if a.iter().all(|x| crate::bsonutil::is_number(x)) {
                return match pos(v) {
                    Some(p) if in_bounds(p) => Ok(()),
                    _ => Err(flat(v)),
                };
            }
            a.iter().try_for_each(check_2dsphere)
        }
        Bson::Document(d) if d.is_empty() => Ok(()),
        Bson::Document(d) => {
            let elem = Bson::Document(d.clone());
            let Ok(t) = d.get_str("type") else {
                return match legacy_doc_point(d, false) {
                    Some(p) if in_bounds(p) => Ok(()),
                    _ => Err(flat(v)),
                };
            };
            let fail = |m: String| Err((m, elem.clone()));
            let c = d.get("coordinates");
            match t {
                "Point" => {
                    let Some(Bson::Array(a)) = c else {
                        let ty = c.map(crate::bsonutil::type_name).unwrap_or("missing");
                        return fail(format!("Point must be an array or object, instead got type {ty}"));
                    };
                    if let Some(bad) = a.iter().find(|x| !crate::bsonutil::is_number(x)) {
                        return fail(format!("Point must only contain numeric elements, instead got type {}", crate::bsonutil::type_name(bad)));
                    }
                    match pos(c.expect("array")) {
                        Some((x, y)) if in_bounds((x, y)) => Ok(()),
                        Some((x, y)) => fail(format!("Longitude/latitude is out of bounds, lng: {x} lat: {y}")),
                        None => fail("Point must contain at least two numeric elements".into()),
                    }
                }
                "LineString" => match c.and_then(positions) {
                    Some(l) if l.len() >= 2 => Ok(()),
                    Some(l) => fail(format!("GeoJSON LineString must have at least 2 vertices, instead got {} vertices: {}", l.len(), shell(c.expect("coords")))),
                    None => fail("GeoJSON coordinates must be an array".into()),
                },
                "Polygon" => {
                    let Some(Bson::Array(loops)) = c else { return fail("Polygon coordinates must be an array".into()) };
                    for l in loops {
                        let Some(ring) = positions(l) else { return fail("Polygon loop must be an array of points".into()) };
                        if ring.len() < 4 {
                            return fail(format!("Loop must have at least 3 different vertices: {}", shell(l)));
                        }
                        if ring.first() != ring.last() {
                            return fail(format!("Loop is not closed, first vertex does not equal last vertex: {}", shell(l)));
                        }
                    }
                    Ok(())
                }
                "MultiPoint" | "MultiLineString" | "MultiPolygon" | "GeometryCollection" => match parse_geojson(d) {
                    Some(_) => Ok(()),
                    None => fail(format!("Can't parse {t}")),
                },
                other => fail(format!("unknown GeoJSON type '{other}'")),
            }
        }
        other => Err((format!("geo element must be an array or object, got {}", crate::bsonutil::type_name(other)), Bson::Document(bson::doc! {"element": other.clone()}))),
    }
}

/// Parse a GeoJSON document (must carry a string `type`).
fn parse_geojson(d: &Document) -> Option<Geometry> {
    let t = d.get_str("type").ok()?;
    let c = d.get("coordinates");
    match t {
        "Point" => Some(Geometry::Point(pos(c?)?)),
        "MultiPoint" => Some(Geometry::MultiPoint(positions(c?)?)),
        "LineString" => {
            let l = positions(c?)?;
            (l.len() >= 2).then_some(Geometry::LineString(l))
        }
        "MultiLineString" => {
            let items = arr(c?)?;
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                let l = positions(it)?;
                if l.len() < 2 {
                    return None;
                }
                out.push(l);
            }
            Some(Geometry::MultiLineString(out))
        }
        "Polygon" => Some(Geometry::Polygon(rings(c?)?)),
        "MultiPolygon" => {
            let items = arr(c?)?;
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                out.push(rings(it)?);
            }
            Some(Geometry::MultiPolygon(out))
        }
        "GeometryCollection" => {
            let items = arr(d.get("geometries")?)?;
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                let Bson::Document(sub) = it else { return None };
                out.push(parse_geojson(sub)?);
            }
            Some(Geometry::GeometryCollection(out))
        }
        _ => None,
    }
}

fn arr(b: &Bson) -> Option<&Vec<Bson>> {
    match b {
        Bson::Array(a) => Some(a),
        _ => None,
    }
}

/// First two numeric elements of an array, both finite.
fn pos(b: &Bson) -> Option<P> {
    let a = arr(b)?;
    if a.len() < 2 {
        return None;
    }
    finite_pos(num_f64(&a[0])?, num_f64(&a[1])?)
}

fn finite_pos(x: f64, y: f64) -> Option<P> {
    (x.is_finite() && y.is_finite()).then_some((x, y))
}

fn positions(b: &Bson) -> Option<Vec<P>> {
    arr(b)?.iter().map(pos).collect()
}

/// A polygon ring: closed, at least 3 distinct vertices.
fn ring(b: &Bson) -> Option<Vec<P>> {
    close_ring(positions(b)?).filter(|r| r.len() >= 4)
}

fn rings(b: &Bson) -> Option<Vec<Vec<P>>> {
    let items = arr(b)?;
    if items.is_empty() {
        return None;
    }
    items.iter().map(ring).collect()
}

fn close_ring(mut r: Vec<P>) -> Option<Vec<P>> {
    if r.is_empty() {
        return None;
    }
    if r.first() != r.last() {
        let f = r[0];
        r.push(f);
    }
    Some(r)
}

/// Point from a document's numeric values in order (first two). `$`-prefixed keys are skipped
/// when `skip_dollar` is set (used for `$near` option documents).
fn legacy_doc_point(d: &Document, skip_dollar: bool) -> Option<P> {
    let mut nums = d
        .iter()
        .filter(|(k, _)| !(skip_dollar && k.starts_with('$')))
        .filter_map(|(_, v)| num_f64(v));
    let x = nums.next()?;
    let y = nums.next()?;
    finite_pos(x, y)
}

/// Great-circle distance in meters between two (lng, lat) points.
pub fn haversine_m(a: (f64, f64), b: (f64, f64)) -> f64 {
    let (lng1, lat1) = (a.0.to_radians(), a.1.to_radians());
    let (lng2, lat2) = (b.0.to_radians(), b.1.to_radians());
    let dlat = lat2 - lat1;
    let dlng = lng2 - lng1;
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlng / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.sqrt().min(1.0).asin()
}

/// Parsed `$near` / `$nearSphere` / `$geoNear` center.
#[derive(Clone, Debug)]
pub struct NearSpec {
    pub point: (f64, f64),
    /// true: distances in meters (GeoJSON point or $nearSphere); false: planar legacy units.
    pub spherical: bool,
    pub max_distance: Option<f64>,
    pub min_distance: Option<f64>,
    /// true only for legacy `$nearSphere [x, y]`: spherical distances are reported in radians
    /// (haversine / EARTH_RADIUS_M), matching `$maxDistance` units. Always false when `spherical`
    /// is false.
    pub radians: bool,
}

fn opt_dist(d: &Document, key: &str) -> Result<Option<f64>> {
    match d.get(key) {
        None => Ok(None),
        Some(b) => match num_f64(b) {
            Some(f) if !f.is_nan() => Ok(Some(f)),
            _ => Err(Error::bad_value(format!("{key} must be a number"))),
        },
    }
}

/// Parse the value of a `$near` / `$nearSphere` operator.
///
/// `$maxDistance` / `$minDistance` are read from inside a document value; sibling operators are
/// set by the caller on the returned spec.
pub fn parse_near(op: &str, value: &Bson) -> Result<NearSpec> {
    let sphere_op = match op {
        "$nearSphere" => true,
        "$near" | "$geoNear" => false,
        _ => return Err(Error::bad_value(format!("unknown near operator '{op}'"))),
    };
    let bad_point = || Error::bad_value("invalid point in geo near query $geometry argument");
    let (point, spherical, radians, max_distance, min_distance) = match value {
        Bson::Document(d) => {
            let max_distance = opt_dist(d, "$maxDistance")?;
            let min_distance = opt_dist(d, "$minDistance")?;
            if let Some(g) = d.get("$geometry") {
                let Bson::Document(gd) = g else { return Err(bad_point()) };
                match parse_geojson(gd) {
                    Some(Geometry::Point(p)) => (p, true, false, max_distance, min_distance),
                    _ => return Err(bad_point()),
                }
            } else {
                let p = legacy_doc_point(d, true).ok_or_else(|| {
                    Error::bad_value("point must be an array or object in geo near query")
                })?;
                (p, sphere_op, sphere_op, max_distance, min_distance)
            }
        }
        Bson::Array(_) => {
            let p = pos(value).ok_or_else(|| {
                Error::bad_value("point must be an array or object in geo near query")
            })?;
            (p, sphere_op, sphere_op, None, None)
        }
        _ => {
            return Err(Error::bad_value(
                "point must be an array or object in geo near query",
            ));
        }
    };
    for d in [max_distance, min_distance].into_iter().flatten() {
        if d < 0.0 {
            return Err(Error::bad_value("maxDistance must be non-negative"));
        }
    }
    Ok(NearSpec { point, spherical, max_distance, min_distance, radians })
}

/// Distance from `spec.point` to the geometry stored in `v` (None if `v` is not a geometry).
///
/// Points are measured directly. For lines and polygons the distance is to the nearest point of
/// the geometry (0 when `spec.point` lies inside a polygon); the nearest point of each segment is
/// computed in planar coordinates and then measured with the spec's metric.
pub fn near_distance(v: &Bson, spec: &NearSpec) -> Option<f64> {
    let g = parse_geometry(v)?;
    let p = spec.point;
    let mut best = f64::INFINITY;
    for prim in prims(&g) {
        let d = match &prim {
            Prim::Point(q) => metric(p, *q, spec),
            Prim::Line(l) => segs_of_line(l)
                .iter()
                .map(|&(a, b)| metric(p, point_seg_dist(p, a, b).0, spec))
                .fold(f64::INFINITY, f64::min),
            Prim::Poly(rings_) => {
                if point_in_rings(p, rings_) {
                    0.0
                } else {
                    rings_
                        .iter()
                        .flat_map(|r| r.windows(2).map(|w| (w[0], w[1])))
                        .map(|(a, b)| metric(p, point_seg_dist(p, a, b).0, spec))
                        .fold(f64::INFINITY, f64::min)
                }
            }
        };
        best = best.min(d);
    }
    best.is_finite().then_some(best)
}

fn metric(a: P, b: P, spec: &NearSpec) -> f64 {
    if spec.spherical {
        let m = haversine_m(a, b);
        if spec.radians { m / EARTH_RADIUS_M } else { m }
    } else {
        (a.0 - b.0).hypot(a.1 - b.1)
    }
}

/// Closest point on segment `ab` to `p`, and its planar distance from `p`.
fn point_seg_dist(p: P, a: P, b: P) -> (P, f64) {
    let d = (b.0 - a.0, b.1 - a.1);
    let len2 = d.0 * d.0 + d.1 * d.1;
    let t = if len2 == 0.0 {
        0.0
    } else {
        (((p.0 - a.0) * d.0 + (p.1 - a.1) * d.1) / len2).clamp(0.0, 1.0)
    };
    let c = (a.0 + t * d.0, a.1 + t * d.1);
    (c, (p.0 - c.0).hypot(p.1 - c.1))
}

/// `$geoWithin` with spec `{$geometry|$box|$polygon|$center|$centerSphere: ..}`.
///
/// The spec is validated even when `v` is not a geometry. A geometry matches when all of its
/// vertices satisfy the region predicate.
pub fn geo_within(v: &Bson, spec: &Document) -> Result<bool> {
    let region = compile_within(spec)?;
    let Some(g) = parse_geometry(v) else { return Ok(false) };
    let verts = vertices(&g);
    Ok(!verts.is_empty() && verts.into_iter().all(|p| region(p)))
}

fn compile_within(spec: &Document) -> Result<Box<dyn Fn(P) -> bool>> {
    let bad = |m: &str| Error::bad_value(m.to_string());
    if spec.len() != 1 {
        return Err(bad("unknown geo within specifier"));
    }
    let (key, val) = spec.iter().next().expect("len checked");
    match key.as_str() {
        "$geometry" => {
            let Bson::Document(gd) = val else {
                return Err(bad("$geoWithin $geometry must be a GeoJSON object"));
            };
            let polys: Vec<Vec<Vec<P>>> = match parse_geojson(gd) {
                Some(Geometry::Polygon(r)) => vec![r],
                Some(Geometry::MultiPolygon(ps)) => ps,
                _ => return Err(bad("$geoWithin $geometry must be a Polygon or MultiPolygon")),
            };
            Ok(Box::new(move |p| polys.iter().any(|r| point_in_rings(p, r))))
        }
        "$box" => {
            let corners = arr(val).ok_or_else(|| bad("$box must be an array of two points"))?;
            if corners.len() != 2 {
                return Err(bad("$box must be an array of two points"));
            }
            let a = pos(&corners[0]).ok_or_else(|| bad("$box points must be [x, y]"))?;
            let b = pos(&corners[1]).ok_or_else(|| bad("$box points must be [x, y]"))?;
            let (x0, x1) = (a.0.min(b.0), a.0.max(b.0));
            let (y0, y1) = (a.1.min(b.1), a.1.max(b.1));
            Ok(Box::new(move |p| p.0 >= x0 && p.0 <= x1 && p.1 >= y0 && p.1 <= y1))
        }
        "$polygon" => {
            let ps = positions(val).ok_or_else(|| bad("$polygon must be an array of [x, y] points"))?;
            let r = close_ring(ps)
                .filter(|r| r.len() >= 4)
                .ok_or_else(|| bad("$polygon must have at least three points"))?;
            Ok(Box::new(move |p| point_in_rings(p, std::slice::from_ref(&r))))
        }
        "$center" | "$centerSphere" => {
            let spherical = key == "$centerSphere";
            let (c, r) = center_args(val, key)?;
            if spherical {
                Ok(Box::new(move |p| haversine_m(p, c) <= r * EARTH_RADIUS_M))
            } else {
                Ok(Box::new(move |p| (p.0 - c.0).hypot(p.1 - c.1) <= r))
            }
        }
        _ => Err(bad("unknown geo within specifier")),
    }
}

fn center_args(val: &Bson, key: &str) -> Result<(P, f64)> {
    let msg = format!("{key} must be [[x, y], radius]");
    let a = arr(val).ok_or_else(|| Error::bad_value(msg.clone()))?;
    if a.len() != 2 {
        return Err(Error::bad_value(msg));
    }
    let c = pos(&a[0]).ok_or_else(|| Error::bad_value(msg.clone()))?;
    let r = num_f64(&a[1])
        .filter(|r| r.is_finite() && *r >= 0.0)
        .ok_or_else(|| Error::bad_value(format!("{key} radius must be a non-negative number")))?;
    Ok((c, r))
}

/// `$geoIntersects` with spec `{$geometry: ..}`.
pub fn geo_intersects(v: &Bson, spec: &Document) -> Result<bool> {
    let mut target = None;
    for (k, val) in spec {
        if k != "$geometry" {
            return Err(Error::bad_value("unknown geo intersects specifier"));
        }
        let Bson::Document(gd) = val else {
            return Err(Error::bad_value("$geometry is not a valid GeoJSON object"));
        };
        target = Some(
            parse_geojson(gd)
                .ok_or_else(|| Error::bad_value("$geometry is not a valid GeoJSON object"))?,
        );
    }
    let target = target.ok_or_else(|| Error::bad_value("$geoIntersects requires $geometry"))?;
    let Some(g) = parse_geometry(v) else { return Ok(false) };
    let (a, b) = (prims(&g), prims(&target));
    Ok(a.iter().any(|x| b.iter().any(|y| prim_intersect(x, y))))
}

/// Flattened primitive components of a geometry.
#[derive(Clone, Debug)]
enum Prim {
    Point(P),
    /// Polyline; a single vertex is a degenerate line.
    Line(Vec<P>),
    /// Rings: outer first, then holes; all closed.
    Poly(Vec<Vec<P>>),
}

fn prims(g: &Geometry) -> Vec<Prim> {
    let mut out = Vec::new();
    push_prims(g, &mut out);
    out
}

fn push_prims(g: &Geometry, out: &mut Vec<Prim>) {
    match g {
        Geometry::Point(p) => out.push(Prim::Point(*p)),
        Geometry::MultiPoint(ps) => out.extend(ps.iter().map(|p| Prim::Point(*p))),
        Geometry::LineString(l) => out.push(Prim::Line(l.clone())),
        Geometry::MultiLineString(ls) => out.extend(ls.iter().map(|l| Prim::Line(l.clone()))),
        Geometry::Polygon(r) => out.push(Prim::Poly(r.clone())),
        Geometry::MultiPolygon(ps) => out.extend(ps.iter().map(|r| Prim::Poly(r.clone()))),
        Geometry::GeometryCollection(gs) => gs.iter().for_each(|g| push_prims(g, out)),
    }
}

/// Every vertex of a geometry.
fn vertices(g: &Geometry) -> Vec<P> {
    prims(g)
        .into_iter()
        .flat_map(|p| match p {
            Prim::Point(q) => vec![q],
            Prim::Line(l) => l,
            Prim::Poly(rs) => rs.into_iter().flatten().collect(),
        })
        .collect()
}

fn segs_of_line(l: &[P]) -> Vec<(P, P)> {
    match l {
        [] => Vec::new(),
        [p] => vec![(*p, *p)],
        _ => l.windows(2).map(|w| (w[0], w[1])).collect(),
    }
}

fn poly_edges(rings_: &[Vec<P>]) -> Vec<(P, P)> {
    rings_.iter().flat_map(|r| r.windows(2).map(|w| (w[0], w[1]))).collect()
}

/// Point-in-polygon with holes; boundary counts as inside.
fn point_in_rings(p: P, rings_: &[Vec<P>]) -> bool {
    let Some(outer) = rings_.first() else { return false };
    if rings_.iter().any(|r| r.windows(2).any(|w| point_seg_dist(p, w[0], w[1]).1 <= EPS)) {
        return true;
    }
    if !crossing_inside(p, outer) {
        return false;
    }
    !rings_[1..].iter().any(|h| crossing_inside(p, h))
}

/// Even-odd ray casting against one closed ring (boundary not special-cased).
fn crossing_inside(p: P, ring_: &[P]) -> bool {
    let mut inside = false;
    let n = ring_.len();
    if n < 2 {
        return false;
    }
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = ring_[i];
        let (xj, yj) = ring_[j];
        if (yi > p.1) != (yj > p.1) && p.0 < (xj - xi) * (p.1 - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

fn orient(a: P, b: P, c: P) -> f64 {
    (b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)
}

fn sgn(x: f64) -> i8 {
    if x > 0.0 {
        1
    } else if x < 0.0 {
        -1
    } else {
        0
    }
}

/// Closed segment intersection (touching and collinear overlap count).
fn seg_intersect(a: P, b: P, c: P, d: P) -> bool {
    let on = |p: P, s: P, e: P| point_seg_dist(p, s, e).1 <= EPS;
    if on(a, c, d) || on(b, c, d) || on(c, a, b) || on(d, a, b) {
        return true;
    }
    let o1 = sgn(orient(a, b, c));
    let o2 = sgn(orient(a, b, d));
    let o3 = sgn(orient(c, d, a));
    let o4 = sgn(orient(c, d, b));
    o1 * o2 < 0 && o3 * o4 < 0
}

fn any_seg_pair(xs: &[(P, P)], ys: &[(P, P)]) -> bool {
    xs.iter().any(|&(a, b)| ys.iter().any(|&(c, d)| seg_intersect(a, b, c, d)))
}

fn prim_intersect(x: &Prim, y: &Prim) -> bool {
    match (x, y) {
        (Prim::Point(p), Prim::Point(q)) => (p.0 - q.0).hypot(p.1 - q.1) <= EPS,
        (Prim::Point(p), Prim::Line(l)) | (Prim::Line(l), Prim::Point(p)) => {
            segs_of_line(l).iter().any(|&(a, b)| point_seg_dist(*p, a, b).1 <= EPS)
        }
        (Prim::Point(p), Prim::Poly(r)) | (Prim::Poly(r), Prim::Point(p)) => point_in_rings(*p, r),
        (Prim::Line(a), Prim::Line(b)) => any_seg_pair(&segs_of_line(a), &segs_of_line(b)),
        (Prim::Line(l), Prim::Poly(r)) | (Prim::Poly(r), Prim::Line(l)) => {
            l.iter().any(|p| point_in_rings(*p, r))
                || any_seg_pair(&segs_of_line(l), &poly_edges(r))
        }
        (Prim::Poly(a), Prim::Poly(b)) => {
            a.iter().flatten().any(|p| point_in_rings(*p, b))
                || b.iter().flatten().any(|p| point_in_rings(*p, a))
                || any_seg_pair(&poly_edges(a), &poly_edges(b))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn g(v: Bson) -> Option<Geometry> {
        parse_geometry(&v)
    }

    #[test]
    fn parse_geojson_forms() {
        assert_eq!(
            g(Bson::Document(doc! {"type": "Point", "coordinates": [1.0, 2.0]})),
            Some(Geometry::Point((1.0, 2.0)))
        );
        assert_eq!(
            g(Bson::Document(doc! {"type": "Point", "coordinates": [1, 2]})),
            Some(Geometry::Point((1.0, 2.0)))
        );
        assert_eq!(
            g(Bson::Document(doc! {"type": "MultiPoint", "coordinates": [[0, 0], [1, 1]]})),
            Some(Geometry::MultiPoint(vec![(0.0, 0.0), (1.0, 1.0)]))
        );
        assert_eq!(
            g(Bson::Document(doc! {"type": "LineString", "coordinates": [[0, 0], [1, 1]]})),
            Some(Geometry::LineString(vec![(0.0, 0.0), (1.0, 1.0)]))
        );
        assert!(g(Bson::Document(doc! {"type": "LineString", "coordinates": [[0, 0]]})).is_none());
        assert!(matches!(
            g(Bson::Document(doc! {"type": "MultiLineString", "coordinates": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]})),
            Some(Geometry::MultiLineString(ls)) if ls.len() == 2
        ));
        // Polygon ring is auto-closed.
        match g(Bson::Document(
            doc! {"type": "Polygon", "coordinates": [[[0, 0], [4, 0], [4, 4], [0, 4]]]},
        )) {
            Some(Geometry::Polygon(r)) => {
                assert_eq!(r[0].len(), 5);
                assert_eq!(r[0].first(), r[0].last());
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(
            g(Bson::Document(doc! {"type": "MultiPolygon", "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 0]]]]})),
            Some(Geometry::MultiPolygon(ps)) if ps.len() == 1
        ));
        assert!(matches!(
            g(Bson::Document(doc! {"type": "GeometryCollection", "geometries": [
                {"type": "Point", "coordinates": [1, 1]},
                {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}
            ]})),
            Some(Geometry::GeometryCollection(gs)) if gs.len() == 2
        ));
        assert!(g(Bson::Document(doc! {"type": "Bogus", "coordinates": [1, 2]})).is_none());
        assert!(g(Bson::Document(doc! {"type": "Point", "coordinates": ["a", 2]})).is_none());
    }

    #[test]
    fn parse_legacy_forms() {
        assert_eq!(g(bson::bson!([3, 4.5])), Some(Geometry::Point((3.0, 4.5))));
        // Extra array elements ignored.
        assert_eq!(g(bson::bson!([3, 4, 99])), Some(Geometry::Point((3.0, 4.0))));
        assert!(g(bson::bson!([3])).is_none());
        assert!(g(bson::bson!(["x", 1])).is_none());
        assert_eq!(
            g(Bson::Document(doc! {"lng": 1, "lat": 2})),
            Some(Geometry::Point((1.0, 2.0)))
        );
        assert_eq!(
            g(Bson::Document(doc! {"x": 5i64, "y": 6.5})),
            Some(Geometry::Point((5.0, 6.5)))
        );
        assert!(g(Bson::Document(doc! {"x": 5})).is_none());
        assert!(g(bson::bson!("nope")).is_none());
    }

    #[test]
    fn haversine_nyc_la() {
        let nyc = (-74.006, 40.7128);
        let la = (-118.2437, 34.0522);
        let d = haversine_m(nyc, la);
        assert!((d - 3_936_000.0).abs() < 10_000.0, "got {d}");
        assert!(haversine_m(nyc, nyc).abs() < 1e-6);
    }

    fn square() -> Bson {
        bson::bson!({"type": "Polygon", "coordinates": [
            [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
            [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]]
        ]})
    }

    #[test]
    fn point_in_polygon_with_hole() {
        let spec = doc! {"$geometry": square()};
        let inside = bson::bson!({"type": "Point", "coordinates": [1, 1]});
        let in_hole = bson::bson!({"type": "Point", "coordinates": [5, 5]});
        let outside = bson::bson!({"type": "Point", "coordinates": [11, 5]});
        assert!(geo_within(&inside, &spec).unwrap());
        assert!(!geo_within(&in_hole, &spec).unwrap());
        assert!(!geo_within(&outside, &spec).unwrap());
        // Boundary is inside.
        let edge = bson::bson!([10, 5]);
        assert!(geo_within(&edge, &spec).unwrap());
        // A polygon entirely inside.
        let small = bson::bson!({"type": "Polygon", "coordinates": [[[1, 1], [2, 1], [2, 2], [1, 1]]]});
        assert!(geo_within(&small, &spec).unwrap());
        // A polygon poking out is not within.
        let poke = bson::bson!({"type": "Polygon", "coordinates": [[[1, 1], [20, 1], [2, 2], [1, 1]]]});
        assert!(!geo_within(&poke, &spec).unwrap());
        // Non-geometry values never match.
        assert!(!geo_within(&bson::bson!("x"), &spec).unwrap());
    }

    #[test]
    fn within_box_center_centersphere_polygon() {
        let p = bson::bson!([1, 1]);
        let far = bson::bson!([50, 50]);
        assert!(geo_within(&p, &doc! {"$box": [[0, 0], [2, 2]]}).unwrap());
        // corners in any order, inclusive
        assert!(geo_within(&bson::bson!([2, 2]), &doc! {"$box": [[2, 2], [0, 0]]}).unwrap());
        assert!(!geo_within(&far, &doc! {"$box": [[0, 0], [2, 2]]}).unwrap());

        assert!(geo_within(&p, &doc! {"$center": [[0, 0], 2.0]}).unwrap());
        assert!(!geo_within(&bson::bson!([3, 0]), &doc! {"$center": [[0, 0], 2.0]}).unwrap());
        assert!(geo_within(&bson::bson!([2, 0]), &doc! {"$center": [[0, 0], 2.0]}).unwrap());

        // 1 degree of latitude ~ 111.2 km; radians 0.02 * R ~ 127 km
        let near = bson::bson!([0, 0.5]);
        let ring = doc! {"$centerSphere": [[0, 0], 0.02]};
        assert!(geo_within(&near, &ring).unwrap());
        assert!(!geo_within(&bson::bson!([0, 2]), &ring).unwrap());

        let tri = doc! {"$polygon": [[0, 0], [10, 0], [0, 10]]};
        assert!(geo_within(&bson::bson!([1, 1]), &tri).unwrap());
        assert!(!geo_within(&bson::bson!([9, 9]), &tri).unwrap());

        // Specifier validation even for non-geometry input.
        assert_eq!(geo_within(&bson::bson!("x"), &doc! {"$box": [[0, 0]]}).unwrap_err().code, 2);
        assert_eq!(geo_within(&p, &doc! {"$center": [[0, 0], -1]}).unwrap_err().code, 2);
        assert_eq!(geo_within(&p, &doc! {"$bogus": 1}).unwrap_err().code, 2);
        assert_eq!(geo_within(&p, &doc! {"$polygon": [[0, 0], [1, 1]]}).unwrap_err().code, 2);
        assert_eq!(
            geo_within(&p, &doc! {"$geometry": {"type": "Point", "coordinates": [0, 0]}})
                .unwrap_err()
                .code,
            2
        );
    }

    #[test]
    fn intersects_line_and_polygon() {
        let line = bson::bson!({"type": "LineString", "coordinates": [[0, 0], [10, 10]]});
        let cross = doc! {"$geometry": {"type": "LineString", "coordinates": [[0, 10], [10, 0]]}};
        assert!(geo_intersects(&line, &cross).unwrap());

        let parallel = doc! {"$geometry": {"type": "LineString", "coordinates": [[0, 1], [9, 10]]}};
        assert!(!geo_intersects(&line, &parallel).unwrap());

        let on_line = bson::bson!({"type": "Point", "coordinates": [5, 5]});
        let off_line = bson::bson!({"type": "Point", "coordinates": [5, 6]});
        let pt_spec = doc! {"$geometry": line.clone()};
        assert!(geo_intersects(&on_line, &pt_spec).unwrap());
        assert!(!geo_intersects(&off_line, &pt_spec).unwrap());

        let poly_spec = doc! {"$geometry": square()};
        // line starting inside the polygon, leaving it: intersects
        assert!(geo_intersects(&line, &poly_spec).unwrap());
        let outside_line = bson::bson!({"type": "LineString", "coordinates": [[20, 0], [30, 0]]});
        assert!(!geo_intersects(&outside_line, &poly_spec).unwrap());
        // line fully inside the hole: does not touch polygon area
        let in_hole = bson::bson!({"type": "LineString", "coordinates": [[4.5, 4.5], [5.5, 5.5]]});
        assert!(!geo_intersects(&in_hole, &poly_spec).unwrap());
        // polygon containing another polygon
        let small = bson::bson!({"type": "Polygon", "coordinates": [[[1, 1], [2, 1], [2, 2], [1, 1]]]});
        assert!(geo_intersects(&small, &poly_spec).unwrap());
        // overlapping polygons with no contained vertex
        let cross_poly = bson::bson!({"type": "Polygon", "coordinates": [[[-5, 4], [15, 4], [15, 6], [-5, 6], [-5, 4]]]});
        assert!(geo_intersects(&cross_poly, &poly_spec).unwrap());
        // disjoint
        let far = bson::bson!({"type": "Point", "coordinates": [100, 100]});
        assert!(!geo_intersects(&far, &poly_spec).unwrap());

        assert_eq!(geo_intersects(&line, &doc! {}).unwrap_err().code, 2);
        assert_eq!(geo_intersects(&line, &doc! {"$geometry": 5}).unwrap_err().code, 2);
    }

    #[test]
    fn near_distance_planar_and_spherical() {
        let planar = parse_near("$near", &bson::bson!([0, 0])).unwrap();
        assert!(!planar.spherical);
        assert!(!planar.radians);
        let d = near_distance(&bson::bson!([3, 4]), &planar).unwrap();
        assert!((d - 5.0).abs() < 1e-9);
        // Line distance (planar): from origin to segment x-axis at (0,3)-(10,3)
        let line = bson::bson!({"type": "LineString", "coordinates": [[0, 3], [10, 3]]});
        assert!((near_distance(&line, &planar).unwrap() - 3.0).abs() < 1e-9);
        // Inside polygon -> 0
        let poly = bson::bson!({"type": "Polygon", "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]]});
        assert_eq!(near_distance(&poly, &planar).unwrap(), 0.0);
        assert!(near_distance(&bson::bson!("x"), &planar).is_none());

        let sph = parse_near(
            "$near",
            &bson::bson!({"$geometry": {"type": "Point", "coordinates": [0, 0]}, "$maxDistance": 200000}),
        )
        .unwrap();
        assert!(sph.spherical);
        assert!(!sph.radians);
        assert_eq!(sph.max_distance, Some(200000.0));
        // 1 degree of longitude at equator ~ R * pi/180 meters
        let d = near_distance(&bson::bson!([1, 0]), &sph).unwrap();
        let expect = EARTH_RADIUS_M * std::f64::consts::PI / 180.0;
        assert!((d - expect).abs() < 1.0, "got {d}");

        let sphere_legacy = parse_near("$nearSphere", &bson::bson!([0, 0])).unwrap();
        assert!(sphere_legacy.spherical && sphere_legacy.radians);
        let rad = near_distance(&bson::bson!([1, 0]), &sphere_legacy).unwrap();
        assert!((rad - std::f64::consts::PI / 180.0).abs() < 1e-9);

        // Errors
        assert_eq!(
            parse_near("$near", &bson::bson!({"$geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}}))
                .unwrap_err()
                .code,
            2
        );
        assert_eq!(
            parse_near("$near", &bson::bson!({"$geometry": {"type": "Point", "coordinates": [0, 0]}, "$maxDistance": -1}))
                .unwrap_err()
                .code,
            2
        );
        assert_eq!(parse_near("$near", &bson::bson!("x")).unwrap_err().code, 2);
    }
}
