package engine

// util.go contains utility functions to make making http requests easier.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

func httpget(addr string) ([]byte, error) {
	resp, err := http.Get(addr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("httpget: got status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func httpdel(addr string) error {
	req, err := http.NewRequest(http.MethodDelete, addr, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent &&
		resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("httpdel: status code is not 204 or 404; got: %d", resp.StatusCode)
	}
	return nil
}

func httpput(addr string, body io.Reader, clen int64) error {
	req, err := http.NewRequest(http.MethodPut, addr, body)
	if err != nil {
		return err
	}
	req.ContentLength = clen

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent &&
		resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("httpput: status code is not 201 or 204; got: %d", resp.StatusCode)
	}
	return nil
}

func httpheader(addr string, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, addr, nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}
