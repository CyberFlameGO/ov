package oviewer

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
)

func TestModel_ReadAll(t *testing.T) {
	type args struct {
		r io.ReadCloser
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				r: ioutil.NopCloser(bytes.NewBufferString("foo\nbar\n")),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := NewModel()
			if err != nil {
				t.Fatal(err)
			}
			if err := m.ReadAll(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("Model.ReadAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
