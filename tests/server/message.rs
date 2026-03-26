/* 
Copyright (c) 2026  NickelAnge.Studio 
Email               mathieu.grenier@nickelange.studio
Git                 https://github.com/NickelAngeStudio/baphonet

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::u8;

use baphonet::Message;





/// Quick serialize macro inspired by my Tampon crate.
macro_rules! serialize {
    ($value:expr, $buffer:expr, $start : expr) => {{

        let bytes = $value.to_le_bytes();
        let _ = &mut $buffer[$start..$start + bytes.len()].copy_from_slice(&bytes);
        bytes.len()

    } as usize };

    (@STRING $value:expr, $buffer:expr, $start : expr) => {{

        let len : u32 = $value.len() as u32;
        $start += serialize!(len, $buffer, $start);

        $buffer[$start..($start + $value.len())].copy_from_slice(&$value.as_bytes());


        $value.len()

    } as usize };
}

/// Quick deserialize macro inspired by my Tampon crate.
macro_rules! deserialize {
    ($name:ident, $type:ty, $buffer:expr, $start : expr) => {
        let $name : $type = <$type>::from_le_bytes($buffer[$start..$start + size_of::<$type>()].try_into().unwrap());
        $start += size_of::<$type>();

    };

    (@STRING $name:ident, $buffer:expr, $start : expr) => {

        let u32_bs = size_of::<u32>();
        let string_size = (<u32>::from_le_bytes($buffer[$start..$start + u32_bs].try_into().unwrap())) as usize;
        let $name : String = String::from_utf8($buffer[$start + u32_bs..$start + u32_bs + string_size].to_vec()).expect("UTF8 String incorrect!"); 
        $start += u32_bs + string_size;

    };
}

/// Create message that can be used for tests
macro_rules! create_test_message {
    ($struct_name : ident) => {
        /// Test Server-To-Client message.
        pub struct $struct_name {
            pub pu8 : u8, pub pu16 : u16,  pub pu32 : u32,  pub pu64 : u64,  pub pu128 : u128,
            pub pstring1 : String, pub pstring2 : String, pub pstring3 : String,
        }

        #[test] 
        #[allow(non_snake_case)]
        fn $struct_name(){

            let pu8 : u8 = u8::MAX / 2;
            let pu16 : u16 = u16::MAX / 2;
            let pu32 : u32 = u32::MAX / 2;
            let pu64 : u64 = u64::MAX / 2;
            let pu128 : u128 = u128::MAX / 2;

            let pstring1 = "Lorem ipsum dolor sit amet.".to_owned();
            let pstring2 = "Excepteur sint occaecat cupidatat non proident!".to_owned();
            let pstring3 = "Itaque earum rerum hic tenetur a sapiente delectus, ut aut reiciendis voluptatibus maiores alias consequatur aut perferendis doloribus asperiores repellat?".to_owned();

            let message = $struct_name::new(pu8, pu16, pu32, pu64, pu128,pstring1.clone(), pstring2.clone(), pstring3.clone());
            let mut buffer : Vec<u8> = Vec::<u8>::with_capacity(u16::MAX as usize);
            buffer.resize(u16::MAX as usize, 0);

            // Serialize message
            message.to_le_bytes(&mut buffer).unwrap();

            // Extract message from buffer
            let message2 = $struct_name::from_le_bytes(&buffer).unwrap();

            // Compare values
            assert_eq!(message.pu8, message2.pu8);
            assert_eq!(message.pu16, message2.pu16);
            assert_eq!(message.pu32, message2.pu32);
            assert_eq!(message.pu64, message2.pu64);
            assert_eq!(message.pu128, message2.pu128);

            assert_eq!(message.pstring1, message2.pstring1);
            assert_eq!(message.pstring2, message2.pstring2);
            assert_eq!(message.pstring3, message2.pstring3);

            
        }

        impl $struct_name {
            pub fn new(pu8 : u8,pu16 : u16,pu32 : u32,pu64 : u64,pu128 : u128, pstring1 : String, pstring2 : String, pstring3 : String) -> $struct_name{
                $struct_name { pu8, pu16, pu32, pu64, pu128, pstring1, pstring2, pstring3 }
            }
        }

        impl Message for $struct_name {
            

            fn to_le_bytes(&self, buffer : &mut [u8]) -> Result<usize, baphonet::Error> {
                
                let mut bytes_count : usize = 0;

                bytes_count += serialize!(self.pu8, buffer, bytes_count);
                bytes_count += serialize!(self.pu16, buffer, bytes_count);
                bytes_count += serialize!(self.pu32, buffer, bytes_count);
                bytes_count += serialize!(self.pu64, buffer, bytes_count);
                bytes_count += serialize!(self.pu128, buffer, bytes_count);


                bytes_count += serialize!(@STRING self.pstring1, buffer, bytes_count);
                bytes_count += serialize!(@STRING self.pstring2, buffer, bytes_count);
                bytes_count += serialize!(@STRING self.pstring3, buffer, bytes_count);


                Ok(bytes_count)

            }

            fn from_le_bytes(buffer : &[u8]) -> Result<Self, baphonet::Error> where Self: Sized {

                let mut _bytes_count : usize = 0;
                deserialize!(pu8, u8, buffer, _bytes_count);
                deserialize!(pu16, u16, buffer, _bytes_count);
                deserialize!(pu32, u32, buffer, _bytes_count);
                deserialize!(pu64, u64, buffer, _bytes_count);
                deserialize!(pu128, u128, buffer, _bytes_count);
                deserialize!(@STRING pstring1, buffer, _bytes_count);
                deserialize!(@STRING pstring2, buffer, _bytes_count);
                deserialize!(@STRING pstring3, buffer, _bytes_count);

                Ok($struct_name{ pu8, pu16, pu32, pu64, pu128, pstring1, pstring2, pstring3 })

                
            }
        }
    }

}


create_test_message!( ServerToClientMessage );
create_test_message!( ClientToServerMessage );