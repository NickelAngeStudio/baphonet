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

/// Server-To-Client message integration.
pub struct ServerToClientMessage {
    pub pu8: u8,
    pub pu16: u16,
    pub pu32: u32,
    pub pu64: u64,
    pub pu128: u128,
    pub pstring1: String,
    pub pstring2: String,
    pub pstring3: String,
}

impl ServerToClientMessage {
    pub fn new(
        pu8: u8,
        pu16: u16,
        pu32: u32,
        pu64: u64,
        pu128: u128,
        pstring1: String,
        pstring2: String,
        pstring3: String,
    ) -> ServerToClientMessage {
        ServerToClientMessage {
            pu8,
            pu16,
            pu32,
            pu64,
            pu128,
            pstring1,
            pstring2,
            pstring3,
        }
    }

    /// Create a control Message for tests
    pub fn control() -> ServerToClientMessage {
        let (pu8, pu16, pu32, pu64, pu128) = (
            u8::MAX / 2,
            u16::MAX / 2,
            u32::MAX / 2,
            u64::MAX / 2,
            u128::MAX / 2,
        );
        let pstring1 = "Lorem ipsum dolor sit amet.".to_owned();
        let pstring2 = "Excepteur sint occaecat cupidatat non proident!".to_owned();
        let pstring3 = "Itaque earum rerum hic tenetur a sapiente delectus, ut aut reiciendis voluptatibus maiores alias consequatur aut perferendis doloribus asperiores repellat?".to_owned();

        ServerToClientMessage::new(
            pu8,
            pu16,
            pu32,
            pu64,
            pu128,
            pstring1.clone(),
            pstring2.clone(),
            pstring3.clone(),
        )
    }
}

impl Message for ServerToClientMessage {
    fn serialize(&self, buffer: &mut [u8]) -> Result<usize, ()> {
        // Use tampon::bytes_size!() to verify if buffer can contain all bytes
        if buffer.len()
            >= tampon::bytes_size!((self.pu8):u8, (self.pu16):u16,
            (self.pu32):u32, (self.pu64):u64, (self.pu128):u128, (self.pstring1, self.pstring2, self.pstring3):String)
        {
            // Use tampon::serialize!() to write parameters to buffer.
            tampon::serialize!(buffer, bytes_count ,(self.pu8):u8, (self.pu16):u16,
                (self.pu32):u32, (self.pu64):u64, (self.pu128):u128, (self.pstring1, self.pstring2, self.pstring3):String);

            Ok(bytes_count)
        } else {
            // Buffer is too small to serialize Message
            Err(())
        }
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, ()>
    where
        Self: Sized,
    {
        // Use tampon::deserialize_size! verify size needed and if buffer is corrupted.
        match tampon::deserialize_size!(buffer, (pu8):u8, (pu16):u16, (pu32):u32, (pu64):u64, (pu128):u128, (pstring1, pstring2, pstring3):String)
        {
            Ok(_) => {
                tampon::deserialize!(buffer, bytes_count ,(pu8):u8, (pu16):u16, (pu32):u32, (pu64):u64, (pu128):u128, (pstring1, pstring2, pstring3):String);
                Ok(ServerToClientMessage {
                    pu8,
                    pu16,
                    pu32,
                    pu64,
                    pu128,
                    pstring1,
                    pstring2,
                    pstring3,
                })
            }
            Err(_) => Err(()), // Buffer is corrupted or incomplete.
        }
    }
}

/// Client-To-Server message integration.
pub struct ClientToServerMessage {
    pub p1: i8,
    pub p2: i8,
    pub p3: i8,
    pub p4: i32,
    pub ps: Vec<String>,
}

impl ClientToServerMessage {
    pub fn new(p1: i8, p2: i8, p3: i8, p4: i32, ps: Vec<String>) -> ClientToServerMessage {
        ClientToServerMessage { p1, p2, p3, p4, ps }
    }

    /// Create a control Message for tests
    pub fn control() -> ClientToServerMessage {
        let (p1, p2, p3, p4) = (i8::MIN / 2, i8::MAX / 2, -1, i32::MIN);
        let ps = vec![
            "Vous êtes les maîtres de l'évasion!".to_owned(),
            "君たちは脱出の達人だ".to_owned(),
            "भवन्तः पलायनस्य स्वामिनः".to_owned(),
            "Ste majstrami úniku".to_owned(),
            "أنتم أسياد الهروب".to_owned(),
        ];

        ClientToServerMessage::new(p1, p2, p3, p4, ps)
    }
}

impl Message for ClientToServerMessage {
    fn serialize(&self, buffer: &mut [u8]) -> Result<usize, ()> {
        // Use tampon::bytes_size!() to verify if buffer can contain all bytes
        if buffer.len()
            >= tampon::bytes_size!((self.p1, self.p2, self.p3):i8, (self.p4):i32,  [self.ps]:String)
        {
            // Use tampon::serialize!() to write parameters to buffer.
            tampon::serialize!(buffer, bytes_count ,(self.p1, self.p2, self.p3):i8, (self.p4):i32, [self.ps]:String);

            Ok(bytes_count)
        } else {
            // Buffer is too small to serialize Message
            Err(())
        }
    }

    fn deserialize(buffer: &[u8]) -> Result<Self, ()>
    where
        Self: Sized,
    {
        // Use tampon::deserialize_size! verify size needed and if buffer is corrupted.
        match tampon::deserialize_size!(buffer, (p1, p2, p3):i8, (p4):i32, [ps]:String) {
            Ok(_) => {
                tampon::deserialize!(buffer, bytes_count ,(p1, p2, p3):i8, (p4):i32, [ps]:String);
                Ok(ClientToServerMessage { p1, p2, p3, p4, ps })
            }
            Err(_) => Err(()), // Buffer is corrupted or incomplete.
        }
    }
}

#[test]
fn server_to_client_message_serialize_buffer_too_small() {
    let message = ServerToClientMessage::control();
    let size_needed = tampon::bytes_size!((message.pu8):u8, (message.pu16):u16, (message.pu32):u32, (message.pu64):u64, (message.pu128):u128,
        (message.pstring1, message.pstring2, message.pstring3):String);
    let mut buffer: Vec<u8> = Vec::<u8>::with_capacity(size_needed - 1);

    match message.serialize(&mut buffer) {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(_) => {}
    }
}

#[test]
fn server_to_client_message_deserialize_buffer_incomplete() {
    let buffer = [0u8, 1, 2, 3, 4, 5, 6, 7, 78];
    match ServerToClientMessage::deserialize(&buffer) {
        Ok(_) => panic!("Shouldn't be ok()!"),
        Err(_) => {}
    }
}

#[test]
fn server_to_client_message_serialize_deserialize() {
    let message = ServerToClientMessage::control();
    let mut buffer: Vec<u8> = Vec::<u8>::with_capacity(u16::MAX as usize);
    buffer.resize(u16::MAX as usize, 0);

    // Serialize message
    message.serialize(&mut buffer).unwrap();

    // Extract message from buffer
    let message2 = ServerToClientMessage::deserialize(&buffer).unwrap();

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

#[test]
fn client_to_server_message_serialize_deserialize() {
    let message = ClientToServerMessage::control();
    let mut buffer: Vec<u8> = Vec::<u8>::with_capacity(u16::MAX as usize);
    buffer.resize(u16::MAX as usize, 0);

    // Serialize message
    message.serialize(&mut buffer).unwrap();

    // Extract message from buffer
    let message2 = ClientToServerMessage::deserialize(&buffer).unwrap();

    // Compare values
    assert_eq!(message.p1, message2.p1);
    assert_eq!(message.p2, message2.p2);
    assert_eq!(message.p3, message2.p3);
    assert_eq!(message.p4, message2.p4);
    assert_eq!(message.ps.len(), message2.ps.len());

    for i in 0..message.ps.len() {
        assert_eq!(message.ps[i], message2.ps[i]);
    }
}
