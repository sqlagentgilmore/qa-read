mod dtconv;

use crate::dtconv::DT_CONV_MAP;
use calamine::{Reader as XlReader, Xlsx};
use polars::prelude::{
    CsvEncoding, DataTypeExpr, Expr, IntoLazy, LazyCsvReader, LazyFileListReader, LazyFrame,
    NamedFrom, NullValues, PlPath, PlSmallStr, Schema, Series,
};
use polars_core::prelude::{AnyValue, DataFrame, DataType};
use qa_settings::Comparable;
use std::marker::PhantomData;
use std::path::Path;
use qa_settings::qa_kind::QaKind;

pub struct Reader<'a, T> {
    inner: Comparable,
    _reader: &'a PhantomData<T>,
}

pub fn get_lazy_frames(
    comp: &Comparable,
) -> Result<(LazyFrame, LazyFrame), Box<dyn std::error::Error>> {
    match comp.kind() {
        QaKind::Txt | QaKind::Csv => Reader {
            inner: comp.clone(),
            _reader: &PhantomData::<PhantomTxtReader>::default(),
        }
        .get_lazy_frames(),
        QaKind::PivotTable(_) => Reader {
            inner: comp.clone(),
            _reader: &PhantomData::<PhantomPivotTableReader>::default(),
        }
        .get_lazy_frames(),
        QaKind::Table(_) => Reader {
            inner: comp.clone(),
            _reader: &PhantomData::<PhantomTableReader>::default(),
        }
        .get_lazy_frames(),
        QaKind::SheetRange(_) => Reader {
            inner: comp.clone(),
            _reader: &PhantomData::<PhantomSheetRangeReader>::default(),
        }
        .get_lazy_frames(),
        _kind => Err(format!("Reader for kind '{}' is not implemented", _kind.as_str_kind()).into()),
    }
}

impl<T> Reader<'_, T> {
    pub fn new(comp: Comparable) -> Self {
        Self {
            inner: comp,
            _reader: &PhantomData,
        }
    }
    pub fn get_lazy_frames<'a>(
        &'a self,
    ) -> Result<(LazyFrame, LazyFrame), Box<dyn std::error::Error>>
    where
        &'a Self: Read,
    {
        let left = self.read(self.inner.left_path())?;
        let right = self.read(self.inner.right_path())?;
        Ok((left, right))
    }
}

struct PhantomTxtReader;
struct PhantomPivotTableReader;
struct PhantomTableReader;
struct PhantomSheetRangeReader;

pub trait Read {
    type Metadata;
    fn read(&self, file: &Path) -> Result<LazyFrame, Box<dyn std::error::Error>>;
    fn schema(&self) -> Result<Schema, Box<dyn std::error::Error>> {
        let raw = self.raw_schema();
        if raw.is_empty() {
            return Err("Read failed due to empty provided schema".into());
        }
        let mut schema = Schema::default();
        for (col_name, type_str) in raw.into_iter() {
            schema.insert(
                col_name.to_string().into(),
                DT_CONV_MAP.get(type_str).unwrap().clone(),
            );
        }
        Ok(schema)
    }
    fn metadata(&self) -> Self::Metadata;
    fn raw_schema(&self) -> &[(String, String)];
}

/// Reads a text or csv file.
impl Read for &'_ Reader<'_, PhantomTxtReader> {
    type Metadata = ();
    fn read(&self, file: &Path) -> Result<LazyFrame, Box<dyn std::error::Error>> {
        let schema = self.schema()?;
        let ignore_columns = schema
            .iter()
            .filter_map(|val| {
                if val.1 == &DataType::Null {
                    None
                } else {
                    Some(Expr::Column(val.0.clone()))
                }
            })
            .collect::<Vec<_>>();
        LazyCsvReader::new(PlPath::from_str(file.to_str().ok_or("Invalid file path")?))
            .with_has_header(self.inner.has_header())
            .with_separator(self.inner.separator())
            .with_rechunk(self.inner.rechunk())
            .with_eol_char(self.inner.eol_char())
            .with_ignore_errors(self.inner.ignore_errors())
            .with_null_values(
                self.inner.null_values().map(|v| {
                    NullValues::AllColumns(v.iter().map(|v| PlSmallStr::from(v)).collect())
                }),
            )
            .with_quote_char(self.inner.quote_char())
            .with_low_memory(self.inner.low_memory())
            .with_encoding(if self.inner.enforce_utf8() {
                CsvEncoding::Utf8
            } else {
                CsvEncoding::LossyUtf8
            })
            .with_skip_rows(self.inner.skip_lines())
            .with_missing_is_null(self.inner.missing_is_null())
            .with_schema(Some(schema.into()))
            .finish()
            .map(|lf| lf.select(ignore_columns))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    fn metadata(&self) -> Self::Metadata {
        todo!()
    }

    fn raw_schema(&self) -> &[(String, String)] {
        self.inner.schema()
    }
}

/// Reads a specific pivot table cache from an Excel file.
impl Read for &'_ Reader<'_, PhantomPivotTableReader> {
    type Metadata = (String, String);
    fn read(&self, file: &Path) -> Result<LazyFrame, Box<dyn std::error::Error>> {
        let meta = self.metadata();
        let mut wb: Xlsx<_> = calamine::open_workbook(file)?;
        let pivot_tables = wb.pivot_tables()?;

        let schema = self.schema()?;
        let schema_len = schema.len();
        // next for each column in schema
        let mut cycle_columns = (0..schema_len).cycle();
        let mut columns = Vec::with_capacity(schema.len());

        let mut rows = wb
            .pivot_table_data(&pivot_tables, &meta.0, &meta.1)
            .map_err(|e| Box::new(e))?;
        if let Some(headers) = rows.next() {
            for header in headers? {
                let column = unsafe { cycle_columns.next().unwrap_unchecked() };
                let (name, dtype) = unsafe { schema.get_at_index(column).unwrap_unchecked() };

                // headers should always be strings
                if calamine::Data::String(name.to_string()) != header {
                    panic!(
                        "Pivot table header '{}' does not match expected schema column name '{}'",
                        header, name
                    );
                } else if dtype == &DataType::Null {
                    columns.push(Vec::<AnyValue>::with_capacity(0));
                } else {
                    columns.push(Vec::<AnyValue>::with_capacity(1000));
                }
            }
            for data in rows {
                for value in data?.iter() {
                    // Safety: cycle_schema is guaranteed to have enough elements because empty schema is checked earlier
                    let column = unsafe { cycle_columns.next().unwrap_unchecked() };
                    let (_, dtype) = unsafe { schema.get_at_index(column).unwrap_unchecked() };

                    if dtype == &DataType::Null {
                        continue;
                    } else {
                        dtconv::cast_excel_type_to_polars_type(value, dtype, &mut columns[column])?;
                    }
                }
            }
            let mut df = DataFrame::default();
            for ((name, dt), values) in schema.into_iter().zip(columns.into_iter()) {
                if dt == DataType::Null {
                    continue;
                } else {
                    df.with_column(Series::new(name, values).cast(&dt)?)?;
                }
            }
            Ok(df.lazy())
        } else {
            // empty pivot table, return empty dataframe with schema
            Ok(LazyFrame::default().with_columns(
                schema
                    .iter()
                    .map(|s| Expr::Column(s.0.clone()).cast(DataTypeExpr::from(s.1.clone())))
                    .collect::<Vec<Expr>>(),
            ))
        }
    }

    fn metadata(&self) -> Self::Metadata {
        if let Some(meta) = self.inner.kind().get_pivot_table_info() {
            (
                meta.sheet_name.to_string(),
                meta.pivot_table_name.to_string(),
            )
        } else {
            panic!("Invalid QaKind for PivotTableReader");
        }
    }

    fn raw_schema(&self) -> &[(String, String)] {
        self.inner.schema()
    }
}

/// Reads a specific table from an Excel file.
impl Read for &'_ Reader<'_, PhantomTableReader> {
    type Metadata = String;
    fn read(&self, file: &Path) -> Result<LazyFrame, Box<dyn std::error::Error>> {
        let meta = self.metadata();
        let mut wb: Xlsx<_> = calamine::open_workbook(file)?;
        let tables = wb.table_by_name(meta.as_str())?;

        let schema = self.schema()?;
        let schema_len = schema.len();
        // next for each column in schema
        let mut cycle_columns = (0..schema_len).cycle();
        let mut columns = Vec::with_capacity(schema.len());
        for row in tables.data().rows() {
            for col in row {
                // Safety: cycle_schema is guaranteed to have enough elements because empty schema is checked earlier
                let column = unsafe { cycle_columns.next().unwrap_unchecked() };
                let (_, dtype) = unsafe { schema.get_at_index(column).unwrap_unchecked() };
                if dtype == &DataType::Null {
                    continue;
                } else {
                    dtconv::cast_excel_type_to_polars_type(col, dtype, &mut columns[column])?;
                }
            }
        }
        let mut df = DataFrame::default();
        for ((name, dt), values) in schema.into_iter().zip(columns.into_iter()) {
            if dt == DataType::Null {
                continue;
            } else {
                df.with_column(Series::new(name, values).cast(&dt)?)?;
            }
        }
        Ok(df.lazy())
    }

    fn metadata(&self) -> Self::Metadata {
        if let Some(meta) = self.inner.kind().get_table_info() {
            meta.table_name.to_string()
        } else {
            panic!("Invalid QaKind for TableReader");
        }
    }

    fn raw_schema(&self) -> &[(String, String)] {
        self.inner.schema()
    }
}

/// Reads a specific range from a sheet in an Excel file.
impl Read for &'_ Reader<'_, PhantomSheetRangeReader> {
    type Metadata = (String, (u32, u32), (u32, u32));
    fn read(&self, file: &Path) -> Result<LazyFrame, Box<dyn std::error::Error>> {
        let meta = self.metadata();
        let mut wb: Xlsx<_> = calamine::open_workbook(file)?;
        let schema = self.schema()?;
        let schema_len = schema.len();
        // next for each column in schema
        let mut cycle_columns = (0..schema_len).cycle();
        let mut columns = Vec::with_capacity(schema.len());
        let reader = wb.worksheet_range(meta.0.as_str())?.range(meta.1, meta.2);
        for row in reader.rows() {
            for col in row {
                // Safety: cycle_schema is guaranteed to have enough elements because empty schema is checked earlier
                let column = unsafe { cycle_columns.next().unwrap_unchecked() };
                let (_, dtype) = unsafe { schema.get_at_index(column).unwrap_unchecked() };
                if dtype == &DataType::Null {
                    continue;
                } else {
                    dtconv::cast_excel_type_to_polars_type(col, dtype, &mut columns[column])?;
                }
            }
        }
        let mut df = DataFrame::default();
        for ((name, dt), values) in schema.into_iter().zip(columns.into_iter()) {
            if dt == DataType::Null {
                continue;
            } else {
                df.with_column(Series::new(name, values).cast(&dt)?)?;
            }
        }
        Ok(df.lazy())
    }

    fn metadata(&self) -> Self::Metadata {
        if let Some(meta) = self.inner.kind().get_sheet_range_info() {
            (
                meta.sheet_name.to_string(),
                (meta.start_row as u32, meta.start_col as u32),
                (meta.end_row as u32, meta.end_col as u32),
            )
        } else {
            panic!("Invalid QaKind for SheetRangeReader");
        }
    }

    fn raw_schema(&self) -> &[(String, String)] {
        self.inner.schema()
    }
}

// pub trait Reader {
//     fn read(&self, comp: &Comparable) -> Result<LazyFrame, Box<dyn std::error::Error>> {
//         match comp.kind() {
//             QaKind::Txt => self.read_txt(),
//             QaKind::PivotTable(pivot_table_lookup) => {
//                 self.read_excel_pivot_table()
//             }
//             QaKind::Table(table_lookup) => self.read_excel_table(),
//             QaKind::SheetRange(range_lookup) => {
//                 self.read_excel_sheet_range(range_lookup.sheet_name.as_str())
//             }
//             _kind => {
//                 unimplemented!(_kind.as_str_kind());
//             }
//         }
//     }
//     fn read_txt(&self) -> Result<LazyFrame, Box<dyn std::error::Error>>;
//     fn read_excel_pivot_table(&self) -> Result<LazyFrame, Box<dyn std::error::Error>>;
//     fn read_excel_table(&self) -> Result<LazyFrame, Box<dyn std::error::Error>>;
//     fn read_excel_sheet_range(&self, sheet: &str, start: (usize, usize), end: (usize, usize)) -> Result<LazyFrame, Box<dyn std::error::Error>>;
// }
