package cmap

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
)

func EncodeString256(data string) []byte {

	bts := []byte(data)

	le := len(bts)

	le8 := uint8(le)

	var ret []byte

	ret = append(ret, byte(le8))
	ret = append(ret, bts...)

	return ret
}

func EncodeString60k(bts []byte) []byte {

	le := len(bts)

	le16 := uint16(le)

	var ret []byte
	var lenByte = make([]byte, 2)
	binary.BigEndian.PutUint16(lenByte, le16)

	ret = append(ret, lenByte...)
	ret = append(ret, bts...)
	return ret
}

func EncodeData(data map[*ValueM]struct{}, field_id string, fields, values map[string]struct{}) []byte {

	if data == nil {
		return nil
	}

	var ret []byte

	for v := range data {
		if v != nil {
			var id []byte
			var fieldsBytes []byte

			if len(v.Keys) != 0 && fields != nil {

				//map[string][]string
				for k2, v2 := range v.Keys {

					if _, ok := fields[k2]; ok || len(fields) == 0 {
						//|len|value|len|Value
						var fs []byte
						//array of strings
						for k3 := range v2 {
							fs = append(fs, EncodeString256(k3)...)
						}

						if k2 == field_id {
							for k4 := range v2 {
								id = EncodeString256(k4)
							}
						}

						//add len to key string
						k2Byte := EncodeString256(k2)
						//add len to values []string
						fs_with_len := EncodeString60k(fs)
						//join key and value
						k2Byte = append(k2Byte, fs_with_len...)
						//add len to key and value
						k2Byte = EncodeString60k(k2Byte)
						//add kv to fields byte
						fieldsBytes = append(fieldsBytes, k2Byte...)
					}
				}
				//encode key
				fieldsBytes = EncodeString60k(fieldsBytes)
			} else {

				fieldsBytes = EncodeString60k(make([]byte, 0))
			}

			var valuesBytes []byte

			if len(v.Values) != 0 && values != nil {

				//map[string][]string
				for k2, v2 := range v.Values {

					if _, ok := values[k2]; ok || len(values) == 0 {

						//add len to key string
						k2Byte := EncodeString256(k2)
						//encode value
						fs, err := json.Marshal(v2)
						if err != nil {
							continue
						}
						//add len to value
						fs_with_len := EncodeString60k(fs)
						//join key and value
						k2Byte = append(k2Byte, fs_with_len...)
						//add len to key and value
						k2Byte = EncodeString60k(k2Byte)
						//add kv to fields byte
						valuesBytes = append(valuesBytes, k2Byte...)
					}
				}
				//encode key
				valuesBytes = EncodeString60k(valuesBytes)
			} else {

				valuesBytes = EncodeString60k(make([]byte, 0))
			}

			if len(id) > 0 {
				var toSend []byte
				toSend = append(toSend, id...)
				toSend = append(toSend, fieldsBytes...)
				toSend = append(toSend, valuesBytes...)
				toSend = EncodeString60k(toSend)

				ret = append(ret, toSend...)
			}
		}
	}

	return ret
}

/*
(2b) all obj len
(1b) len id
(un) id
(2b) len fields
[

	(2b) len fields key value
	(1b) len  fields map key
	(un) fields key
	(2b) len fields map value

[[

	(1b) len fields value single
	(un) value single fields value

]]
(2b) len values
[

	(2b) len values key value
	(1b) len  values map key
	(un) values map key
	(2b) len values map value
	(un) values map value

]
*/

func GetDataFieldTag1(data []byte, field []byte, pointer int) ([]byte, error) {

	if data == nil {
		return nil, errors.New("nil data provided")
	}

	//check for id
	_, next, err := DecodeNextUint8Data(data, pointer)
	if err != nil {
		return nil, err
	}

	//get fields map
	fieldsData, _, err := DecodeNextUint16Data(data, next)
	if err != nil {
		return nil, err
	}

	fieldsPointer := 0

	for fieldsPointer < len(fieldsData) {

		var kvData []byte
		kvData, fieldsPointer, err = DecodeNextUint16Data(fieldsData, fieldsPointer)
		if err != nil {
			return nil, err
		}

		kvKeyData, kvKeyPointer, err := DecodeNextUint8Data(kvData, 0)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(kvKeyData, field) {
			kvValueData, _, err := DecodeNextUint16Data(kvData, kvKeyPointer)
			if err != nil {
				return nil, err
			}
			firstkvValueData, _, err := DecodeNextUint8Data(kvValueData, 0)
			if err != nil {
				return nil, err
			}

			return firstkvValueData, nil

		}

	}

	return nil, errors.New("provided field not in field data")
}

func GetDataValue(data []byte, key []byte, pointer int) ([]byte, error) {

	if data == nil {
		return nil, errors.New("nil data provided")
	}

	//check for id
	_, next, err := DecodeNextUint8Data(data, pointer)
	if err != nil {
		return nil, err
	}

	//get fields map
	_, next, err = DecodeNextUint16Data(data, next)
	if err != nil {
		return nil, err
	}

	valuesData, _, err := DecodeNextUint16Data(data, next)
	if err != nil {
		return nil, err
	}

	valuesPointer := 0

	for valuesPointer < len(valuesData) {

		var kvData []byte
		kvData, valuesPointer, err = DecodeNextUint16Data(valuesData, valuesPointer)
		if err != nil {
			return nil, err
		}

		kvKeyData, kvKeyPointer, err := DecodeNextUint8Data(kvData, 0)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(kvKeyData, key) {
			kvValueData, _, err := DecodeNextUint16Data(kvData, kvKeyPointer)
			if err != nil {
				return nil, err
			}

			return kvValueData, nil

		}

	}

	return nil, errors.New("provided field not in values data")
}

func GetDataID(data []byte, pointer int) ([]byte, error) {

	if data == nil {
		return nil, errors.New("nil data provided")
	}

	ret, _, err := DecodeNextUint8Data(data, pointer)

	return ret, err
}

func DecodeNextUint8Data(data []byte, pointer int) (ret []byte, next int, err error) {

	if data == nil {
		return nil, 0, errors.New("nil data provided")
	}

	if len(data) < pointer+1 {
		return nil, 0, errors.New("data too short")
	}

	id_length := uint8(data[pointer])
	if id_length == 0 {
		return nil, pointer + 1, nil
	}

	id_start := pointer + 1

	if len(data) < pointer+1+int(id_length) {
		return nil, 0, errors.New("data too short to contain tag: " + strconv.Itoa(len(data)) + " checked length: " + strconv.Itoa((pointer + 1 + int(id_length))))
	}

	id_byte := data[id_start : id_start+int(id_length)]

	return id_byte, id_start + int(id_length), nil
}

func DecodeNextUint16Data(data []byte, pointer int) (ret []byte, next int, err error) {

	if data == nil {
		return nil, 0, errors.New("nil data provided")
	}

	if len(data) < (pointer + 1) {
		return nil, 0, errors.New("data too short")
	}

	id_length := binary.BigEndian.Uint16(data[pointer : pointer+2])
	if id_length == 0 {
		return nil, pointer + 2, nil
	}

	id_start := pointer + 2

	if len(data) < (pointer + 1 + int(id_length)) {
		return nil, 0, errors.New("data too short to contain tag 16 length of data: " + strconv.Itoa(len(data)) + " checked length: " + strconv.Itoa((pointer + int(id_length))))

	}

	id_byte := data[id_start : id_start+int(id_length)]

	return id_byte, pointer + int(id_length) + 2, nil
}

func RemoveNextUint16Data(data []byte, pointer int) (ret []byte, next int, err error) {

	if data == nil {
		return nil, 0, errors.New("nil data provided")
	}

	if len(data) < (pointer + 1) {
		return nil, 0, errors.New("data too short")
	}

	id_length := binary.BigEndian.Uint16(data[pointer : pointer+2])
	if id_length == 0 {
		return nil, pointer + 2, nil
	}

	id_start := pointer + 2

	if len(data) < (pointer + 1 + int(id_length)) {
		return nil, 0, errors.New("data too short to contain tag 16 length of data: " + strconv.Itoa(len(data)) + " checked length: " + strconv.Itoa((pointer + 1 + int(id_length))))

	}

	id_byte := data[pointer : id_start+int(id_length)]

	return id_byte, pointer + int(id_length) + 2, nil
}
