package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	io "io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"time"

	"github.com/pkg/errors"
)

func PostFiles(hc *http.Client, servURL string, imgs [][]byte, respObj interface{}) (duration time.Duration, err error) {
	var resp *http.Response
	var part io.Writer
	t0 := time.Now()
	reqBody := &bytes.Buffer{}
	writer := multipart.NewWriter(reqBody)
	for i, img := range imgs {
		//part, err := writer.CreateFormFile("data", "image.jpg") //generates "Content-Type: application/octet-stream"
		partHeader := textproto.MIMEHeader{}
		partHeader.Add("Content-Disposition", fmt.Sprintf("form-data; name=\"data\"; filename=\"image%s.jpg\"", i))
		partHeader.Add("Content-Type", "image/jpeg")
		if part, err = writer.CreatePart(partHeader); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		if _, err = part.Write(img); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	writer.Close()
	req, err := http.NewRequest("POST", servURL, reqBody)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	if resp, err = hc.Do(req); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	duration = time.Since(t0)
	var respBody []byte
	respBody, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if err = json.Unmarshal(respBody, respObj); err != nil {
		err = errors.Wrapf(err, "failed to decode respBody: %+v", string(respBody))
		return
	}
	return
}
