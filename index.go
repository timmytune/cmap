package cmap

import (
	"errors"
	"fmt"
	"sync"
)

func JoinErrors(errs ...error) error {
	var joinErrsR func(string, int, ...error) error
	joinErrsR = func(soFar string, count int, errs ...error) error {
		if len(errs) == 0 {
			if count == 0 {
				return nil
			}
			return fmt.Errorf(soFar)
		}
		current := errs[0]
		next := errs[1:]
		if current == nil {
			return joinErrsR(soFar, count, next...)
		}
		count++
		if count == 1 {
			return joinErrsR(fmt.Sprintf("%s", current), count, next...)
		} else if count == 2 {
			return joinErrsR(fmt.Sprintf("1: %s\n2: %s", soFar, current), count, next...)
		}
		return joinErrsR(fmt.Sprintf("%s\n%d: %s", soFar, count, current), count, next...)
	}
	return joinErrsR("", 0, errs...)
}

type Value struct {
	Keys   map[string]string
	Values map[string]interface{}
	sync.RWMutex
}

type FieldValue struct {
	data []*Value

	sync.RWMutex
}

type Field struct {
	data map[string]*FieldValue
	sync.RWMutex
}

type Accesser struct {
	c      *Cmap
	V      *Value
	Fv     *FieldValue
	Filter []*Value
	err    error
}

func (a *Accesser) Set(field string, key string) (ac *Accesser) {
	ac = a

	if a.err != nil {
		return
	}

	if a.V == nil {
		a.err = errors.New("Accesser.Set must have value set ")
		return
	}

	if field == "" {
		ac.err = errors.New("empty field provided in function Accesser.Set")
		return
	}

	if key == "" {
		ac.err = errors.New("empty key provided in function Accesser.Set")
		return
	}

	a.c.RLock()
	f, ok := a.c.data[field]
	a.c.RUnlock()

	if !ok {

		f := Field{}
		f.data = make(map[string]*FieldValue)

		fv := FieldValue{}
		fv.data = make([]*Value, 0)

		//setting
		a.V.Lock()
		a.V.Keys[field] = key
		a.V.Unlock()

		fv.data = append(fv.data, a.V)

		f.data[key] = &fv

		a.c.Lock()
		a.c.data[field] = &f
		a.c.Unlock()

		a.Fv = &fv

		return
	}

	f.RLock()
	fv, ok := f.data[key]
	f.RUnlock()

	if !ok {
		fv := FieldValue{}
		fv.data = make([]*Value, 0)

		//setting
		a.V.Lock()
		a.V.Keys[field] = key
		a.V.Unlock()

		fv.data = append(fv.data, a.V)

		f.Lock()
		f.data[key] = &fv
		f.Unlock()

		a.Fv = &fv

		return
	}

	vInFv := false

	fv.Lock()
	for _, v1 := range fv.data {
		if v1 == a.V {
			vInFv = true
			break
		}
	}
	if !vInFv {

		a.V.Lock()
		a.V.Keys[field] = key
		a.V.Unlock()

		fv.data = append(fv.data, a.V)
	}
	fv.Unlock()

	a.Fv = fv

	return
}

func (a *Accesser) FilterKV(f map[string]string) (ac *Accesser) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Value, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			pass := true

			v.RLock()
			for kf, vf := range f {
				if v.Keys[kf] != vf {
					pass = false
				}
			}
			v.RUnlock()

			if pass {
				filter = append(filter, v)
			}
		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.data {

			pass := true

			v.RLock()
			for kf, vf := range f {
				if v.Keys[kf] != vf {
					pass = false
				}
			}
			v.RUnlock()

			if pass {
				filter = append(filter, v)
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesser) FilterNoKey(key string) (ac *Accesser) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Value, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if !ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.data {
			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if !ok {
				filter = append(filter, v)
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesser) FilterHasKey(key string) (ac *Accesser) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Value, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.data {
			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if ok {
				filter = append(filter, v)
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesser) FilterHasValue(key string) (ac *Accesser) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Value, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.data {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if ok {
				filter = append(filter, v)
			}

		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesser) FilterNotValue(key string) (ac *Accesser) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Value, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if !ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.data {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if !ok {
				filter = append(filter, v)
			}

		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesser) SetValue(key string, val interface{}) (ac *Accesser) {
	ac = a

	if a.err != nil {
		return
	}

	if key == "" {
		a.err = errors.New("empty key provided in function Accesser.SetValue")
		return
	}

	if a.V == nil {
		a.err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	a.V.Values[key] = val
	a.V.Unlock()

	return
}

func (a *Accesser) GetValue(key string) (val interface{}, err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if key == "" {
		a.err = errors.New("empty key provided in function Accesser.GetValue")
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	ok := false

	a.V.RLock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	val, ok = a.V.Values[key]
	a.V.RUnlock()

	if !ok {
		a.err = errors.New("V does not have the value" + key + "set")
		err = a.err
	}

	return
}

func (a *Accesser) GetValueString(key string) (vaa string, err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.RLock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	val, ok := a.V.Values[key]
	a.V.RUnlock()

	if !ok {
		return "", nil
	}

	vaa, ok = val.(string)
	if !ok {
		return "", errors.New("data in interface not string")
	}

	return
}

func (a *Accesser) GetField(key string) (val string, err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	ok := false

	a.V.RLock()
	if a.V.Keys == nil {
		a.V.Keys = make(map[string]string)
	}
	val, ok = a.V.Keys[key]
	a.V.RUnlock()

	if !ok {
		a.err = errors.New("value does not have the key '" + key + "' set")
		err = a.err
	}

	return
}

func (a *Accesser) DeleteValue(key string) (err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	delete(a.V.Values, key)
	a.V.Unlock()

	return
}

func (a *Accesser) DeleteField(field string) (err error) {
	if field == "" {
		a.err = errors.New("field cannot be empty")
		err = a.err
		return
	}

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	key := a.V.Keys[field]
	delete(a.V.Keys, field)
	a.V.Unlock()

	if key == "" {
		return
	}

	a.c.RLock()
	f, ok := a.c.data[field]
	a.c.RUnlock()

	if !ok {
		return
	}

	f.RLock()
	fv, ok := f.data[key]
	f.RUnlock()

	if !ok {
		return
	}

	delFIeldVal := false

	fv.Lock()

	fvlen := len(fv.data)

	if fvlen == 1 && fv.data[0] == a.V {
		delFIeldVal = true
	} else if fvlen == 1 && fv.data[0] != a.V {
		a.err = errors.New("fieldvalue with this key" + key + " does not have the current value attached ")
		err = a.err
	} else if fvlen > 1 {
		for k, v := range fv.data {
			if v == a.V {
				if k != len(fv.data)-1 {
					fv.data[k] = fv.data[len(fv.data)-1]
				}
				fv.data = fv.data[:len(fv.data)-1]
				break
			}
		}
	} else if fvlen == 0 {
		delFIeldVal = true
	}

	fv.Unlock()

	if delFIeldVal {
		f.Lock()
		delete(f.data, key)
		f.Unlock()
	}

	return
}

func (a *Accesser) Delete() (err error) {

	if a.err != nil {
		err = a.err
		return
	}

	errs := make([]error, 0)

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	keys := a.V.Keys
	a.V.Unlock()

	for field, key := range keys {

		a.c.RLock()
		f, ok := a.c.data[field]
		a.c.RUnlock()

		if !ok {
			continue
		}

		f.RLock()
		fv, ok := f.data[key]
		f.RUnlock()

		if !ok {
			continue
		}

		delFIeldVal := false

		fv.Lock()

		fvlen := len(fv.data)

		if fvlen == 1 && fv.data[0].Keys[field] == a.V.Keys[field] {
			delFIeldVal = true
		} else if fvlen == 1 && fv.data[0] != a.V {
			//The only field should be deletable
			a.err = errors.New("fieldvalue with this key" + key + " does not have the current value attached ")
			err = a.err
			errs = append(errs, err)
		} else if fvlen > 1 {

			for k, v := range fv.data {
				if v == a.V {
					if k != len(fv.data)-1 {
						fv.data[k] = fv.data[len(fv.data)-1]
					}
					fv.data = fv.data[:len(fv.data)-1]
					break
				}
			}
		} else if fvlen == 0 {
			delFIeldVal = true
		} else {
			a.err = errors.New("fieldvalue with this key" + key + " has issue deleting ")
			errs = append(errs, err)
			err = a.err
		}

		fv.Unlock()

		if delFIeldVal {
			f.Lock()
			delete(f.data, key)
			f.Unlock()
		}

	}

	if len(errs) > 0 {
		err = JoinErrors(errs...)
	}

	return
}

func (a *Accesser) Err() (err error) {
	err = a.err
	return
}

func (a *Accesser) FilterCount() int {
	return len(a.Filter)
}

func (a *Accesser) ValueCount() int {
	a.Fv.RLock()
	l := len(a.Filter)
	a.Fv.Unlock()
	return l
}

func (a *Accesser) IsValue() bool {
	if a.V == nil {
		return false
	} else {
		return true
	}
}

type Cmap struct {
	data map[string]*Field
	sync.RWMutex
}

func (c *Cmap) Field(field string, key string) (ac *Accesser) {

	ac = &Accesser{c: c}

	if field == "" {
		ac.err = errors.New("empty field provided in function Cmap.Field")
		return
	}

	if key == "" {
		ac.err = errors.New("empty key provided in function Cmap.Field")
		return
	}

	c.RLock()
	f, ok := c.data[field]
	c.RUnlock()

	if !ok {

		f := Field{}
		f.data = make(map[string]*FieldValue)

		fv := FieldValue{}
		fv.data = make([]*Value, 0)

		v := Value{}
		v.Keys = make(map[string]string)

		//setting
		v.Keys[field] = key

		fv.data = append(fv.data, &v)

		f.data[key] = &fv

		c.Lock()
		c.data[field] = &f
		c.Unlock()

		ac.Fv = &fv
		ac.V = &v

		return
	}

	//check fieldData

	f.RLock()
	fv, ok := f.data[key]
	f.RUnlock()

	if !ok {
		fv := FieldValue{}
		fv.data = make([]*Value, 0)

		v := Value{}
		v.Keys = make(map[string]string)

		//setting
		v.Keys[field] = key
		fv.data = append(fv.data, &v)

		f.Lock()
		f.data[key] = &fv
		f.Unlock()

		ac.Fv = &fv
		ac.V = &v

		return
	} else {
		fv.RLock()
		if len(fv.data) == 1 {
			ac.V = fv.data[0]
		}
		fv.RUnlock()
	}

	ac.Fv = fv

	return
}

func (c *Cmap) FieldLen(field string) int {
	c.RLock()
	da, ok := c.data[field]
	c.RUnlock()
	if !ok {
		return 0
	}
	da.RLock()
	ret := len(da.data)
	da.RUnlock()
	return ret
}

func (c *Cmap) FieldIterate(field string, fun func(*map[string]*FieldValue)) {
	c.RLock()
	da, ok := c.data[field]
	c.RUnlock()
	if !ok {
		return
	}
	da.RLock()
	fun(&da.data)
	da.RUnlock()
}

func (c *Cmap) FieldCopy(field string) (ret []string) {
	ret = make([]string, 0)
	c.RLock()
	da, ok := c.data[field]
	c.RUnlock()
	if !ok {
		return
	}
	da.RLock()
	for k := range da.data {
		ret = append(ret, k)
	}
	da.RUnlock()
	return
}

func NewCmap() *Cmap {
	ret := Cmap{}
	ret.data = make(map[string]*Field)
	return &ret
}
