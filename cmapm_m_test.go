package cmap

import (
	"log"
	"strconv"
	"testing"
)

var cmM *CmapM

func TestOneM(t *testing.T) {

	all := 1000000

	for i := 0; i < all; i++ {
		cmM.Field("email", "yinka"+strconv.Itoa(i)+"@yihan.org.ng").Set("phone", "0815486401"+strconv.Itoa(i)).SetValue("index", i).SetValue("test", "testing")

		if i%10 == 0 {
			cmM.Field("email", "yinka"+strconv.Itoa(i)+"@yihan.org.ng").Set("email", "tosin"+strconv.Itoa(i)+"@yihan.org.ng")
		}
	}

	count := cmM.Field("phone", "08154864018").ValueCount()
	if count != 2 {
		t.Error("field count should be 2 got: ", count)
	}

	ct, err := cmM.Field("phone", "08154864018").GetValueString("test")
	if err != nil {
		t.Error("get field testing: ", err)
	}

	if ct != "testing" {
		t.Error("field test did not return testing returned: ", ct)
	}

	fields, err := cmM.Field("email", "tosin100@yihan.org.ng").GetField("phone")
	if err != nil {
		t.Error("get field phone got error: ", err)
	}

	if _, ok := fields["0815486401100"]; !ok {
		t.Error("phone fields does not contain this phone : ", fields)
	}

	fields, err = cmM.Field("email", "tosin100@yihan.org.ng").GetField("email")
	if err != nil {
		t.Error("get field email got error: ", err)
	}

	if len(fields) != 2 {
		t.Error("email field not 2 : ", fields)
	}

	le := cmM.FieldLen("phone")
	if le != all {
		t.Error("field length not 100,000 GOT: ", le)
	}

	err = cmM.Field("phone", "08154864012").DeleteField("phone")
	if err != nil {
		t.Error("delete field threw error: ", err)
	}

	phone, _ := cmM.Field("email", "yinka2@yihan.org.ng").GetField("phone")
	if len(phone) > 0 {
		t.Error("phone not deleted: ", phone)
	}

	email, err := cmM.Field("phone", "08154864013").GetField("email")
	if err != nil {
		t.Error("ger field email threw error: ", err)
	}

	if len(email) != 1 {
		t.Error("email not found: ", email)
	}

	err2 := cmM.Field("phone", "081548640116").Delete()
	if err2 != nil {
		t.Error("error deleting: ", err2)
	}

	le = cmM.FieldLen("phone")
	if le != (all - 2) {
		t.Error("field length not 100,000 GOT: ", le)
	}

	re, err := cmM.Field("phone", "08154864011").GetValue("index")
	if err != nil {
		t.Error("get value index error: ", err)
	} else {
		if re.(int) != 1 {
			t.Error("expected 1 got", re)
		}
	}

	for _, v := range cmM.FieldCopy("phone") {
		err = cmM.Field("phone", v).Delete()
		if err != nil {
			t.Error("delete field in for threw error: ", err)
		}
	}

	le = cmM.FieldLen("phone")
	if le != 0 {
		t.Error("field length not 0 GOT: ", le)
		log.Print("data: ", cmM.data)
	}

}
