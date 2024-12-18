mod abort_multipart_upload;
mod complete_multipart_upload;
mod create_multipart_upload;
mod upload_part;

pub use abort_multipart_upload::abort_multipart_upload;
pub use complete_multipart_upload::complete_multipart_upload;
pub use create_multipart_upload::create_multipart_upload;
pub use upload_part::upload_part;

pub fn get_part_prefix(mut key: String, part_number: u64) -> String {
    assert_eq!(Some('\0'), key.pop());
    key.push('#');
    // if part number is 0, we treat it as object key
    if part_number != 0 {
        // part numbers range is [1, 10000], which can be encoded as 4 digits
        // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
        let part_str = format!("{:04}", part_number - 1);
        key.push_str(&part_str);
    }
    key
}

pub fn parse_part_number(mpu_key: &str, key: &str) -> u32 {
    let mut part_str = mpu_key.to_owned().split_off(key.len());
    part_str.pop(); // remove trailing '\0'
    part_str.parse::<u32>().unwrap() + 1
}
