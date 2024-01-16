package cmap

import (
	"os"
	"strconv"
	"testing"
	"time"
)

var cm *Cmap

func TestMain(m *testing.M) {

	cm = NewCmap()

	cmm = NewCmapm()

	cmM = NewCmapM()

	for i := 0; i < 1000; i++ {
		cm.Field("email", "yinka"+strconv.Itoa(i)+"@yihan.org.ng").Set("phone", "0815486401"+strconv.Itoa(i)).SetValue("index", i).SetValue("test", "testing")
	}

	ret := m.Run()

	time.Sleep(time.Second * 5)

	os.Exit(ret)

}

func TestGet(t *testing.T) {

	re, err := cm.Field("phone", "08154864011").GetValue("index")
	if err != nil {
		t.Error(err)
	} else {
		if re.(int) != 1 {
			t.Error("expected 1 got", re)
		}
	}

	re, err = cm.Field("email", "yinka1@yihan.org.ng").GetValue("index")
	if err != nil {
		t.Error(err)
	} else {
		if re.(int) != 1 {
			t.Error("expected 1 got", re)
		}
	}

}

func TestSetGet(t *testing.T) {

	re := cm.Field("email", "yinka1@yihan.org.ng").Set("name", "yinka").SetValue("surname", "adedoyin").Set("linked", "1")
	if re.Err() != nil {
		t.Error(re.Err())
	}

	re = cm.Field("email", "yinka2@yihan.org.ng").Set("linked", "1")
	if re.Err() != nil {
		t.Error(re.Err())
	}

	ints := cm.data["linked"].data["1"]
	if ints != nil {
		le := len(ints.data)
		if le != 2 {
			t.Error("Field length should be two")
		}
	} else {
		t.Error("Field should not be nil")
	}

	name, err := cm.Field("phone", "08154864011").GetValue("surname")
	if err != nil {
		t.Error(err)
	} else {
		if name.(string) != "adedoyin" {
			t.Error("expected adedoyin got", re)
		}
	}
}

func TestDelete(t *testing.T) {

	time.Sleep(time.Second * 2)
	err := cm.Field("email", "yinka1@yihan.org.ng").Delete()
	if err != nil {
		t.Error(err)
	}

	deleted := cm.data["email"].data["yinka1@yihan.org.ng"]

	if deleted != nil {
		t.Error("email not deleted")
	}

	deleted = cm.data["phone"].data["08154864011"]

	if deleted != nil {
		t.Error("email not deleted")
	}

	deleted = cm.data["email"].data["yinka2@yihan.org.ng"]

	if deleted == nil {
		t.Error("email deleted")
	}

	deleted = cm.data["linked"].data["1"]

	if deleted != nil {
		if len(deleted.data) != 1 {
			t.Error("should be one deleted left")
		}
	} else {
		t.Error("email deleted")
	}

}
