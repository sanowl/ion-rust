use std::mem;

use bumpalo::collections::Vec as BumpVec;
use bumpalo::Bump as BumpAllocator;
use bytes::BufMut;
use delegate::delegate;
use num_bigint::Sign;
use num_traits::Zero;

use crate::binary::decimal::DecimalBinaryEncoder;
use crate::binary::timestamp::TimestampBinaryEncoder;
use crate::binary::uint;
use crate::binary::uint::DecodedUInt;
use crate::binary::var_uint::VarUInt;
use crate::lazy::encoder::binary::v1_0::container_writers::{
    BinaryContainerWriter_1_0, BinaryListValuesWriter_1_0, BinaryListWriter_1_0,
    BinarySExpValuesWriter_1_0, BinarySExpWriter_1_0, BinaryStructFieldsWriter_1_0,
    BinaryStructWriter_1_0,
};
use crate::lazy::encoder::private::Sealed;
use crate::lazy::encoder::value_writer::{AnnotatableValueWriter, ValueWriter};
use crate::raw_symbol_token_ref::AsRawSymbolTokenRef;
use crate::result::{EncodingError, IonFailure};
use crate::types::integer::IntData;
use crate::{Decimal, Int, IonError, IonResult, IonType, RawSymbolTokenRef, SymbolId, Timestamp};

/// The largest possible 'L' (length) value that can be written directly in a type descriptor byte.
/// Larger length values will need to be written as a VarUInt following the type descriptor.
pub(crate) const MAX_INLINE_LENGTH: usize = 13;

pub struct BinaryValueWriter_1_0<'value, 'top> {
    allocator: &'top BumpAllocator,
    encoding_buffer: &'value mut BumpVec<'top, u8>,
}

impl<'value, 'top> BinaryValueWriter_1_0<'value, 'top> {
    pub fn new(
        allocator: &'top BumpAllocator,
        encoding_buffer: &'value mut BumpVec<'top, u8>,
    ) -> BinaryValueWriter_1_0<'value, 'top> {
        BinaryValueWriter_1_0 {
            allocator,
            encoding_buffer,
        }
    }

    #[inline]
    fn push_byte(&mut self, byte: u8) {
        self.encoding_buffer.push(byte);
    }

    #[inline]
    fn push_bytes(&mut self, bytes: &[u8]) {
        self.encoding_buffer.extend_from_slice(bytes)
    }

    pub(crate) fn buffer(&self) -> &[u8] {
        self.encoding_buffer.as_slice()
    }

    pub fn write_symbol_id(mut self, symbol_id: SymbolId) -> IonResult<()> {
        const SYMBOL_BUFFER_SIZE: usize = mem::size_of::<u64>();
        let mut buffer = [0u8; SYMBOL_BUFFER_SIZE];
        let mut writer = std::io::Cursor::new(&mut buffer).writer();
        let encoded_length = DecodedUInt::write_u64(&mut writer, symbol_id as u64)?;

        let type_descriptor: u8;
        if encoded_length <= MAX_INLINE_LENGTH {
            type_descriptor = 0x70 | encoded_length as u8;
            self.push_byte(type_descriptor);
        } else {
            type_descriptor = 0x7E;
            self.push_byte(type_descriptor);
            VarUInt::write_u64(self.encoding_buffer, encoded_length as u64)?;
        }
        let raw_buffer = writer.into_inner().into_inner();
        self.push_bytes(&raw_buffer[..encoded_length]);
        Ok(())
    }

    pub fn write_lob(mut self, value: &[u8], type_code: u8) -> IonResult<()> {
        let encoded_length = value.len();
        let type_descriptor: u8;
        if encoded_length <= MAX_INLINE_LENGTH {
            type_descriptor = type_code | encoded_length as u8;
            self.push_byte(type_descriptor);
        } else {
            type_descriptor = type_code | 0x0E;
            self.push_byte(type_descriptor);
            VarUInt::write_u64(self.encoding_buffer, encoded_length as u64)?;
        }
        self.push_bytes(value);
        Ok(())
    }

    pub fn write_null(mut self, ion_type: IonType) -> IonResult<()> {
        let byte: u8 = match ion_type {
            IonType::Null => 0x0F,
            IonType::Bool => 0x1F,
            IonType::Int => 0x2F,
            IonType::Float => 0x4F,
            IonType::Decimal => 0x5F,
            IonType::Timestamp => 0x6F,
            IonType::Symbol => 0x7F,
            IonType::String => 0x8F,
            IonType::Clob => 0x9F,
            IonType::Blob => 0xAF,
            IonType::List => 0xBF,
            IonType::SExp => 0xCF,
            IonType::Struct => 0xDF,
        };
        self.push_byte(byte);
        Ok(())
    }

    pub fn write_bool(mut self, value: bool) -> IonResult<()> {
        let byte: u8 = if value { 0x11 } else { 0x10 };
        self.push_byte(byte);
        Ok(())
    }

    pub fn write_i64(mut self, value: i64) -> IonResult<()> {
        // Get the absolute value of the i64 and store it in a u64.
        let magnitude: u64 = value.unsigned_abs();
        let encoded = uint::encode_u64(magnitude);
        let bytes_to_write = encoded.as_bytes();

        // The encoded length will never be larger than 8 bytes, so it will
        // always fit in the Int's type descriptor byte.
        let encoded_length = bytes_to_write.len();
        let type_descriptor: u8 = if value >= 0 {
            0x20 | (encoded_length as u8)
        } else {
            0x30 | (encoded_length as u8)
        };
        self.push_byte(type_descriptor);
        self.push_bytes(bytes_to_write);

        Ok(())
    }

    pub fn write_int(mut self, value: &Int) -> IonResult<()> {
        // If the `value` is an `i64`, use `write_i64` and return.
        let value = match &value.data {
            IntData::I64(i) => return self.write_i64(*i),
            IntData::BigInt(i) => i,
        };

        // From here on, `value` is a `BigInt`.
        if value.is_zero() {
            self.push_byte(0x20);
            return Ok(());
        }

        let (sign, magnitude_be_bytes) = value.to_bytes_be();

        let mut type_descriptor: u8 = match sign {
            Sign::Plus | Sign::NoSign => 0x20,
            Sign::Minus => 0x30,
        };

        let encoded_length = magnitude_be_bytes.len();
        if encoded_length <= 13 {
            type_descriptor |= encoded_length as u8;
            self.push_byte(type_descriptor);
        } else {
            type_descriptor |= 0xEu8;
            self.push_byte(type_descriptor);
            VarUInt::write_u64(self.encoding_buffer, encoded_length as u64)?;
        }

        self.push_bytes(magnitude_be_bytes.as_slice());

        Ok(())
    }

    pub fn write_f32(mut self, value: f32) -> IonResult<()> {
        if value == 0f32 && !value.is_sign_negative() {
            self.push_byte(0x40);
            return Ok(());
        }

        self.push_byte(0x44);
        self.push_bytes(&value.to_be_bytes());
        Ok(())
    }

    pub fn write_f64(mut self, value: f64) -> IonResult<()> {
        if value == 0f64 && !value.is_sign_negative() {
            self.push_byte(0x40);
            return Ok(());
        }

        self.push_byte(0x48);
        self.push_bytes(&value.to_be_bytes());
        Ok(())
    }

    pub fn write_decimal(self, value: &Decimal) -> IonResult<()> {
        let _encoded_size = self.encoding_buffer.encode_decimal_value(value)?;
        Ok(())
    }

    pub fn write_timestamp(self, value: &Timestamp) -> IonResult<()> {
        let _ = self.encoding_buffer.encode_timestamp_value(value)?;
        Ok(())
    }

    pub fn write_string<A: AsRef<str>>(mut self, value: A) -> IonResult<()> {
        let text: &str = value.as_ref();
        let encoded_length = text.len(); // The number of utf8 bytes

        let type_descriptor: u8;
        if encoded_length <= MAX_INLINE_LENGTH {
            type_descriptor = 0x80 | encoded_length as u8;
            self.push_byte(type_descriptor);
        } else {
            type_descriptor = 0x8E;
            self.push_byte(type_descriptor);
            VarUInt::write_u64(self.encoding_buffer, encoded_length as u64)?;
        }
        self.push_bytes(text.as_bytes());
        Ok(())
    }

    pub fn write_symbol<A: AsRawSymbolTokenRef>(self, value: A) -> IonResult<()> {
        match value.as_raw_symbol_token_ref() {
            RawSymbolTokenRef::SymbolId(sid) => self.write_symbol_id(sid),
            RawSymbolTokenRef::Text(text) => IonResult::illegal_operation(format!(
                "the Ion 1.0 raw binary writer cannot write text symbols (here: '{text}')"
            )),
        }
    }

    pub fn write_clob<A: AsRef<[u8]>>(self, value: A) -> IonResult<()> {
        let bytes: &[u8] = value.as_ref();
        // The clob type descriptor's high nibble is type code 9
        self.write_lob(bytes, 0x90)
    }

    pub fn write_blob<A: AsRef<[u8]>>(self, value: A) -> IonResult<()> {
        let bytes: &[u8] = value.as_ref();
        // The blob type descriptor's high nibble is type code 10 (0xA)
        self.write_lob(bytes, 0xA0)
    }

    fn list_writer(&mut self) -> BinaryListWriter_1_0<'_, 'top> {
        const LIST_TYPE_CODE: u8 = 0xB0;
        BinaryListWriter_1_0::new(BinaryContainerWriter_1_0::new(
            LIST_TYPE_CODE,
            self.allocator,
            self.encoding_buffer,
        ))
    }

    fn sexp_writer(&mut self) -> BinarySExpWriter_1_0<'_, 'top> {
        const SEXP_TYPE_CODE: u8 = 0xC0;
        BinarySExpWriter_1_0::new(BinaryContainerWriter_1_0::new(
            SEXP_TYPE_CODE,
            self.allocator,
            self.encoding_buffer,
        ))
    }

    fn struct_writer(&mut self) -> BinaryStructWriter_1_0<'_, 'top> {
        const STRUCT_TYPE_CODE: u8 = 0xD0;
        BinaryStructWriter_1_0::new(BinaryContainerWriter_1_0::new(
            STRUCT_TYPE_CODE,
            self.allocator,
            self.encoding_buffer,
        ))
    }

    fn write_list<
        F: for<'a> FnOnce(&mut <Self as ValueWriter>::ListWriter<'a>) -> IonResult<()>,
    >(
        mut self,
        list_fn: F,
    ) -> IonResult<()> {
        self.list_writer().write_values(list_fn)
    }
    fn write_sexp<
        F: for<'a> FnOnce(&mut <Self as ValueWriter>::SExpWriter<'a>) -> IonResult<()>,
    >(
        mut self,
        sexp_fn: F,
    ) -> IonResult<()> {
        self.sexp_writer().write_values(sexp_fn)
    }
    fn write_struct<
        F: for<'a> FnOnce(&mut <Self as ValueWriter>::StructWriter<'a>) -> IonResult<()>,
    >(
        mut self,
        struct_fn: F,
    ) -> IonResult<()> {
        self.struct_writer().write_fields(struct_fn)
    }
}

impl<'value, 'top> Sealed for BinaryValueWriter_1_0<'value, 'top> {}

impl<'value, 'top> ValueWriter for BinaryValueWriter_1_0<'value, 'top> {
    type ListWriter<'a> = BinaryListValuesWriter_1_0<'a>;
    type SExpWriter<'a> = BinarySExpValuesWriter_1_0<'a>;
    type StructWriter<'a> = BinaryStructFieldsWriter_1_0<'a>;

    delegate! {
        to self {
            fn write_null(self, ion_type: IonType) -> IonResult<()>;
            fn write_bool(self, value: bool) -> IonResult<()>;
            fn write_i64(self, value: i64) -> IonResult<()>;
            fn write_int(self, value: &Int) -> IonResult<()>;
            fn write_f32(self, value: f32) -> IonResult<()>;
            fn write_f64(self, value: f64) -> IonResult<()>;
            fn write_decimal(self, value: &Decimal) -> IonResult<()>;
            fn write_timestamp(self, value: &Timestamp) -> IonResult<()>;
            fn write_string(self, value: impl AsRef<str>) -> IonResult<()>;
            fn write_symbol(self, value: impl AsRawSymbolTokenRef) -> IonResult<()>;
            fn write_clob(self, value: impl AsRef<[u8]>) -> IonResult<()>;
            fn write_blob(self, value: impl AsRef<[u8]>) -> IonResult<()>;
            fn write_list<F: for<'a> FnOnce(&mut Self::ListWriter<'a>) -> IonResult<()>>(
                self,
                list_fn: F,
            ) -> IonResult<()>;
            fn write_sexp<F: for<'a> FnOnce(&mut Self::SExpWriter<'a>) -> IonResult<()>>(
                self,
                sexp_fn: F,
            ) -> IonResult<()>;
            fn write_struct<
                F: for<'a> FnOnce(&mut Self::StructWriter<'a>) -> IonResult<()>,
            >(
                self,
                struct_fn: F,
            ) -> IonResult<()>;
        }
    }
}

pub struct BinaryAnnotatableValueWriter_1_0<'value, 'top> {
    allocator: &'top BumpAllocator,
    encoding_buffer: &'value mut BumpVec<'top, u8>,
}

impl<'value, 'top> BinaryAnnotatableValueWriter_1_0<'value, 'top> {
    pub fn new(
        allocator: &'top BumpAllocator,
        encoding_buffer: &'value mut BumpVec<'top, u8>,
    ) -> BinaryAnnotatableValueWriter_1_0<'value, 'top> {
        BinaryAnnotatableValueWriter_1_0 {
            allocator,
            encoding_buffer,
        }
    }
}

impl<'value, 'top: 'value> AnnotatableValueWriter
    for BinaryAnnotatableValueWriter_1_0<'value, 'top>
{
    type ValueWriter = BinaryValueWriter_1_0<'value, 'top>;
    type AnnotatedValueWriter<'a, SymbolType: AsRawSymbolTokenRef + 'a> =
    BinaryAnnotationsWrapperWriter<'a, 'top, SymbolType> where Self: 'a;
    fn with_annotations<'a, SymbolType: AsRawSymbolTokenRef>(
        self,
        annotations: &'a [SymbolType],
    ) -> Self::AnnotatedValueWriter<'a, SymbolType>
    where
        Self: 'a,
    {
        BinaryAnnotationsWrapperWriter::new(self.allocator, annotations, self.encoding_buffer)
    }

    #[inline(always)]
    fn without_annotations(self) -> BinaryValueWriter_1_0<'value, 'top> {
        BinaryValueWriter_1_0::new(self.allocator, self.encoding_buffer)
    }
}

pub struct BinaryAnnotationsWrapperWriter<'value, 'top, SymbolType: AsRawSymbolTokenRef> {
    annotations: &'value [SymbolType],
    allocator: &'top BumpAllocator,
    output_buffer: &'value mut BumpVec<'top, u8>,
}

impl<'value, 'top, SymbolType: AsRawSymbolTokenRef>
    BinaryAnnotationsWrapperWriter<'value, 'top, SymbolType>
{
    pub fn new(
        allocator: &'top BumpAllocator,
        annotations: &'value [SymbolType],
        encoding_buffer: &'value mut BumpVec<'top, u8>,
    ) -> BinaryAnnotationsWrapperWriter<'value, 'top, SymbolType> {
        BinaryAnnotationsWrapperWriter {
            annotations,
            allocator,
            output_buffer: encoding_buffer,
        }
    }
}

impl<'value, 'top, SymbolType: AsRawSymbolTokenRef>
    BinaryAnnotationsWrapperWriter<'value, 'top, SymbolType>
{
    fn encode_annotated<F>(self, encode_value_fn: F) -> IonResult<()>
    where
        F: for<'a> FnOnce(BinaryAnnotatedValueWriter_1_0<'a, 'top>) -> IonResult<()>,
    {
        let allocator = self.allocator;
        let buffer = allocator.alloc_with(|| BumpVec::new_in(allocator));
        {
            let annotated_value_writer =
                BinaryAnnotatedValueWriter_1_0::new(self.allocator, buffer);
            encode_value_fn(annotated_value_writer)?;
        }
        self.annotate_encoded_value(buffer.as_slice())
    }

    fn annotate_encoded_value(self, encoded_value: &[u8]) -> IonResult<()> {
        let mut encoded_annotations_sequence = BumpVec::new_in(self.allocator);
        self.encode_annotations_sequence(&mut encoded_annotations_sequence)?;

        let mut encoded_annotations_sequence_length = BumpVec::new_in(self.allocator);
        VarUInt::write_u64(
            &mut encoded_annotations_sequence_length,
            encoded_annotations_sequence.len() as u64,
        )?;

        let total_length = encoded_annotations_sequence.len()
            + encoded_annotations_sequence_length.len()
            + encoded_value.len();

        if total_length <= MAX_INLINE_LENGTH {
            self.output_buffer.push(0xE0u8 | total_length as u8);
        } else {
            self.output_buffer.push(0xEEu8);
            VarUInt::write_u64(self.output_buffer, total_length as u64)?;
        }

        self.output_buffer
            .extend_from_slice(encoded_annotations_sequence_length.as_slice());
        self.output_buffer
            .extend_from_slice(encoded_annotations_sequence.as_slice());
        self.output_buffer.extend_from_slice(encoded_value);

        Ok(())
    }

    fn encode_annotations_sequence(&self, buffer: &'_ mut BumpVec<'_, u8>) -> IonResult<()> {
        for annotation in self.annotations {
            let RawSymbolTokenRef::SymbolId(sid) = annotation.as_raw_symbol_token_ref() else {
                return Err(IonError::Encoding(EncodingError::new(
                    "binary Ion 1.0 cannot encode text literal annotations",
                )));
            };
            VarUInt::write_u64(buffer, sid as u64)?;
        }
        Ok(())
    }
}

impl<'value, 'top, SymbolType: AsRawSymbolTokenRef> Sealed
    for BinaryAnnotationsWrapperWriter<'value, 'top, SymbolType>
{
    // No methods, precludes implementations outside the crate.
}

/// Takes a series of `TYPE => METHOD` pairs, generating a function for each that calls the
/// corresponding value writer method and then prefixes the encoded result with an annotations wrapper.
macro_rules! delegate_and_annotate {
    // End of iteration
    () => {};
    // Recurses one argument pair at a time
    ($value_type:ty => $method:ident, $($rest:tt)*) => {
        fn $method(self, value: $value_type) -> IonResult<()> {
            self.encode_annotated(|value_writer| value_writer.$method(value))
        }
        delegate_and_annotate!($($rest)*);
    };
}

impl<'value, 'top, SymbolType: AsRawSymbolTokenRef> ValueWriter
    for BinaryAnnotationsWrapperWriter<'value, 'top, SymbolType>
{
    type ListWriter<'a> = BinaryListValuesWriter_1_0<'a>;
    type SExpWriter<'a> = BinarySExpValuesWriter_1_0<'a>;

    type StructWriter<'a> = BinaryStructFieldsWriter_1_0<'a>;

    delegate_and_annotate!(
        IonType => write_null,
        bool => write_bool,
        i64 => write_i64,
        &Int => write_int,
        f32 => write_f32,
        f64 => write_f64,
        &Decimal => write_decimal,
        &Timestamp => write_timestamp,
        impl AsRef<str> => write_string,
        impl AsRawSymbolTokenRef => write_symbol,
        impl AsRef<[u8]> => write_clob,
        impl AsRef<[u8]> => write_blob,
    );

    fn write_list<F: for<'a> FnOnce(&mut Self::ListWriter<'a>) -> IonResult<()>>(
        self,
        list_fn: F,
    ) -> IonResult<()> {
        self.encode_annotated(|value_writer| value_writer.write_list(list_fn))
    }
    fn write_sexp<F: for<'a> FnOnce(&mut Self::SExpWriter<'a>) -> IonResult<()>>(
        self,
        sexp_fn: F,
    ) -> IonResult<()> {
        self.encode_annotated(|value_writer| value_writer.write_sexp(sexp_fn))
    }
    fn write_struct<F: for<'a> FnOnce(&mut Self::StructWriter<'a>) -> IonResult<()>>(
        self,
        struct_fn: F,
    ) -> IonResult<()> {
        self.encode_annotated(|value_writer| value_writer.write_struct(struct_fn))
    }
}

pub struct BinaryAnnotatedValueWriter_1_0<'value, 'top> {
    allocator: &'top BumpAllocator,
    // Note that unlike the BinaryValueWriter_1_0, the borrow and the BumpVec here have the same
    // lifetime. This allows this type to be passed as a closure argument.
    buffer: &'value mut BumpVec<'top, u8>,
}

impl<'value, 'top> BinaryAnnotatedValueWriter_1_0<'value, 'top> {
    pub fn new(allocator: &'top BumpAllocator, buffer: &'value mut BumpVec<'top, u8>) -> Self {
        Self { allocator, buffer }
    }
    pub(crate) fn value_writer(&mut self) -> BinaryValueWriter_1_0<'_, 'top> {
        BinaryValueWriter_1_0::new(self.allocator, self.buffer)
    }

    pub(crate) fn buffer(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

impl<'value, 'top> Sealed for BinaryAnnotatedValueWriter_1_0<'value, 'top> {}

impl<'value, 'top: 'value> ValueWriter for BinaryAnnotatedValueWriter_1_0<'value, 'top> {
    type ListWriter<'a> = BinaryListValuesWriter_1_0<'a>;
    type SExpWriter<'a> = BinarySExpValuesWriter_1_0<'a>;
    type StructWriter<'a> = BinaryStructFieldsWriter_1_0<'a>;
    delegate! {
        to self.value_writer() {
            fn write_null(mut self, ion_type: IonType) -> IonResult<()>;
            fn write_bool(mut self, value: bool) -> IonResult<()>;
            fn write_i64(mut self, value: i64) -> IonResult<()>;
            fn write_int(mut self, value: &Int) -> IonResult<()>;
            fn write_f32(mut self, value: f32) -> IonResult<()>;
            fn write_f64(mut self, value: f64) -> IonResult<()>;
            fn write_decimal(mut self, value: &Decimal) -> IonResult<()>;
            fn write_timestamp(mut self, value: &Timestamp) -> IonResult<()>;
            fn write_string(mut self, value: impl AsRef<str>) -> IonResult<()>;
            fn write_symbol(mut self, value: impl AsRawSymbolTokenRef) -> IonResult<()>;
            fn write_clob(mut self, value: impl AsRef<[u8]>) -> IonResult<()>;
            fn write_blob(mut self, value: impl AsRef<[u8]>) -> IonResult<()>;
            fn write_list<F: for<'a> FnOnce(&mut Self::ListWriter<'a>) -> IonResult<()>>(
                mut self,
                list_fn: F,
            ) -> IonResult<()>;
            fn write_sexp<F: for<'a> FnOnce(&mut Self::SExpWriter<'a>) -> IonResult<()>>(
                mut self,
                sexp_fn: F,
            ) -> IonResult<()>;
            fn write_struct<
                F: for<'a> FnOnce(&mut Self::StructWriter<'a>) -> IonResult<()>,
            >(
                mut self,
                struct_fn: F,
            ) -> IonResult<()>;
        }
    }
}
#[cfg(test)]
mod tests {
    use crate::lazy::encoder::annotate::Annotate;
    use crate::lazy::encoder::binary::v1_0::writer::LazyRawBinaryWriter_1_0;
    use crate::lazy::encoder::value_writer::AnnotatableValueWriter;
    use crate::lazy::encoder::write_as_ion::WriteAsSExp;
    use crate::raw_symbol_token_ref::AsRawSymbolTokenRef;
    use crate::{Element, IonData, IonResult, RawSymbolTokenRef, Timestamp};

    fn writer_test(
        expected: &str,
        mut test: impl FnMut(&mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>) -> IonResult<()>,
    ) -> IonResult<()> {
        let expected = Element::read_all(expected)?;
        let mut buffer = Vec::new();
        let mut writer = LazyRawBinaryWriter_1_0::new(&mut buffer)?;
        test(&mut writer)?;
        writer.flush()?;
        let actual = Element::read_all(buffer)?;
        assert!(
            IonData::eq(&expected, &actual),
            "Actual \n    {actual:?}\nwas not equal to\n    {expected:?}\n"
        );
        Ok(())
    }

    #[test]
    fn write_scalars() -> IonResult<()> {
        let expected = r#"
            1
            false
            3e0
            "foo"
            name
            2023-11-09T
            {{4AEA6g==}}
        "#;
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            writer
                .write(1)?
                .write(false)?
                .write(3f32)?
                .write("foo")?
                .write(RawSymbolTokenRef::SymbolId(4))?
                .write(Timestamp::with_ymd(2023, 11, 9).build()?)?
                .write([0xE0u8, 0x01, 0x00, 0xEA])?;
            Ok(())
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_empty_list() -> IonResult<()> {
        let expected = "[]";
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            let value_writer = writer.value_writer();
            value_writer
                .without_annotations()
                .write_list(|_list| Ok(()))
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_list() -> IonResult<()> {
        let expected = r#"
            [
                1,
                false,
                3e0,
                "foo",
                name,
                2023-11-09T,
                {{4AEA6g==}},
                // Nested list
                [1, 2, 3],
            ]
        "#;
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            writer
                .value_writer()
                .without_annotations()
                .write_list(|list| {
                    list.write(1)?
                        .write(false)?
                        .write(3f32)?
                        .write("foo")?
                        .write(RawSymbolTokenRef::SymbolId(4))?
                        .write(Timestamp::with_ymd(2023, 11, 9).build()?)?
                        .write([0xE0u8, 0x01, 0x00, 0xEA])?
                        .write([1, 2, 3])?;
                    Ok(())
                })
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_empty_sexp() -> IonResult<()> {
        let expected = "()";
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            writer
                .value_writer()
                .without_annotations()
                .write_sexp(|_sexp| Ok(()))
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_sexp() -> IonResult<()> {
        let expected = r#"
            (
                1
                false
                3e0
                "foo"
                name
                2023-11-09T
                {{4AEA6g==}}
                // Nested list
                [1, 2, 3]
            )
        "#;
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            writer
                .value_writer()
                .without_annotations()
                .write_sexp(|sexp| {
                    sexp.write(1)?
                        .write(false)?
                        .write(3f32)?
                        .write("foo")?
                        .write(RawSymbolTokenRef::SymbolId(4))?
                        .write(Timestamp::with_ymd(2023, 11, 9).build()?)?
                        .write([0xE0u8, 0x01, 0x00, 0xEA])?
                        .write([1, 2, 3])?;
                    Ok(())
                })
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_empty_struct() -> IonResult<()> {
        let expected = "{}";
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            writer
                .value_writer()
                .without_annotations()
                .write_struct(|_struct| Ok(()))
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_struct() -> IonResult<()> {
        let expected = r#"
            // This test uses symbol ID field names because the raw writer has no symbol table.
            {
                $0: 1,
                $1: false,
                $2: 3e0,
                $3: "foo",
                $4: name,
                $5: 2023-11-09T,
                $6: {{4AEA6g==}},
                // Nested list
                $7: [1, 2, 3],
            }
        "#;
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            writer
                .value_writer()
                .without_annotations()
                .write_struct(|struct_| {
                    struct_
                        .write(0, 1)?
                        .write(1, false)?
                        .write(2, 3f32)?
                        .write(3, "foo")?
                        .write(4, RawSymbolTokenRef::SymbolId(4))?
                        .write(5, Timestamp::with_ymd(2023, 11, 9).build()?)?
                        .write(6, [0xE0u8, 0x01, 0x00, 0xEA])?
                        .write(7, [1, 2, 3])?;
                    Ok(())
                })
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_annotated_scalars() -> IonResult<()> {
        let expected = r#"
        // The raw writer doesn't have a symbol table, so this only uses symbols
        // that are already in the system symbol table.
            name::1
            version::false
            imports::symbols::3e0
            max_id::version::"foo"
            $ion::$4
            $ion_symbol_table::2023-11-09T
            $ion_1_0::{{4AEA6g==}}
        "#;
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            writer
                .write(1.annotated_with(&[4]))?
                .write(false.annotated_with(&[5]))?
                .write(3f32.annotated_with(&[6, 7]))?
                .write("foo".annotated_with(&[8, 5]))?
                .write(4usize.as_raw_symbol_token_ref().annotated_with(&[1]))?
                .write(
                    Timestamp::with_ymd(2023, 11, 9)
                        .build()?
                        .annotated_with(&[3]),
                )?
                .write((&[0xE0u8, 0x01, 0x00, 0xEA][..]).annotated_with(&[2]))?;
            Ok(())
        };
        writer_test(expected, test)
    }

    #[test]
    fn write_annotated_containers() -> IonResult<()> {
        let expected = r#"
            []
            $4::[]
            $4::[1, 2, 3]
            $4::$7::[1, 2, 3]
            $4::$7::[
                $4::$7::[1, 2, 3]
            ]
            ()
            $4::()
            $4::(1 2 3)
            $4::$7::()
            $4::$7::(
                $4::$7::(1 2 3)
            )
        "#;
        let test = |writer: &mut LazyRawBinaryWriter_1_0<&mut Vec<u8>>| {
            let empty_sequence: &[i32] = &[];
            // []
            writer.write(empty_sequence)?;

            // $4::[]
            writer.write(empty_sequence.annotated_with(&[4]))?;

            // $4::[1, 2, 3]
            writer.write([1, 2, 3].annotated_with(&[4]))?;

            // $4::$7::[1, 2, 3]
            writer.write([1, 2, 3].annotated_with(&[4, 7]))?;

            // $4::$7::[
            //      $4::$7::[1, 2, 3]
            // ]
            writer.write([[1, 2, 3].annotated_with(&[4, 7])].annotated_with(&[4, 7]))?;

            // ()
            writer.write(empty_sequence.as_sexp())?;

            // $4::()
            writer.write(empty_sequence.as_sexp().annotated_with(&[4]))?;

            // $4::(1 2 3)
            writer.write([1, 2, 3].as_sexp().annotated_with(&[4]))?;

            // $4::$7::()
            writer.write(empty_sequence.as_sexp().annotated_with(&[4, 7]))?;

            // $4::$7::(
            //   $4::$7::(1 2 3)
            // )
            writer.write(
                [[1, 2, 3].as_sexp().annotated_with(&[4, 7])]
                    .as_sexp()
                    .annotated_with(&[4, 7]),
            )?;

            Ok(())
        };
        writer_test(expected, test)
    }
}
