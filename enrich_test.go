package ago

import (
	"context"
	"errors"
	"testing"

	"github.com/zoobzio/capitan"
)

func TestEnrich(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	userKey := capitan.NewKey[string]("user", "User name")

	enrich := Enrich[Order, string]("fetch-user", userKey, func(_ context.Context, o Order) (string, error) {
		return "user-" + o.ID, nil
	})

	flow := NewFlow(Order{ID: "order-123"}, signal)
	result, err := enrich.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Enrich failed: %v", err)
	}

	// Check the field was set
	user, ok := From(result, userKey)
	if !ok {
		t.Error("expected user field to be set")
	}
	if user != "user-order-123" {
		t.Errorf("expected 'user-order-123', got %q", user)
	}
}

func TestEnrich_WithError(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	userKey := capitan.NewKey[string]("user", "User name")
	expectedErr := errors.New("fetch failed")

	enrich := Enrich[Order, string]("fetch-user", userKey, func(_ context.Context, _ Order) (string, error) {
		return "", expectedErr
	})

	flow := NewFlow(Order{ID: "order-123"}, signal)
	_, err := enrich.Process(ctx, flow)

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestEnrichOptional(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	userKey := capitan.NewKey[string]("user", "User name")

	enrich := EnrichOptional[Order, string]("fetch-user", userKey, func(_ context.Context, o Order) (string, error) {
		return "user-" + o.ID, nil
	})

	flow := NewFlow(Order{ID: "order-456"}, signal)
	result, err := enrich.Process(ctx, flow)

	if err != nil {
		t.Fatalf("EnrichOptional failed: %v", err)
	}

	user, ok := From(result, userKey)
	if !ok {
		t.Error("expected user field to be set")
	}
	if user != "user-order-456" {
		t.Errorf("expected 'user-order-456', got %q", user)
	}
}

func TestEnrichOptional_WithError(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")
	userKey := capitan.NewKey[string]("user", "User name")
	enrichErr := errors.New("enrichment failed")

	enrich := EnrichOptional[Order, string]("fetch-user", userKey, func(_ context.Context, _ Order) (string, error) {
		return "", enrichErr
	})

	flow := NewFlow(Order{ID: "order-789"}, signal)
	result, err := enrich.Process(ctx, flow)

	// Should not return error (optional enrichment)
	if err != nil {
		t.Fatalf("EnrichOptional should not fail: %v", err)
	}

	// Error should be added to flow's error list
	if len(result.Errors) != 1 {
		t.Fatalf("expected 1 error in flow, got %d", len(result.Errors))
	}
	if !errors.Is(result.Errors[0], enrichErr) {
		t.Errorf("expected error %v, got %v", enrichErr, result.Errors[0])
	}

	// Field should not be set
	_, ok := From(result, userKey)
	if ok {
		t.Error("expected user field to not be set on error")
	}
}

func TestEnrich_ComplexType(t *testing.T) {
	ctx := context.Background()
	signal := capitan.NewSignal("test", "Test")

	type UserInfo struct {
		Name  string
		Email string
	}
	userInfoKey := capitan.NewKey[UserInfo]("user_info", "User information")

	enrich := Enrich[Order, UserInfo]("fetch-user-info", userInfoKey, func(_ context.Context, o Order) (UserInfo, error) {
		return UserInfo{
			Name:  "User for " + o.ID,
			Email: o.ID + "@example.com",
		}, nil
	})

	flow := NewFlow(Order{ID: "order-abc"}, signal)
	result, err := enrich.Process(ctx, flow)

	if err != nil {
		t.Fatalf("Enrich failed: %v", err)
	}

	userInfo, ok := From(result, userInfoKey)
	if !ok {
		t.Error("expected user_info field to be set")
	}
	if userInfo.Name != "User for order-abc" {
		t.Errorf("expected name 'User for order-abc', got %q", userInfo.Name)
	}
	if userInfo.Email != "order-abc@example.com" {
		t.Errorf("expected email 'order-abc@example.com', got %q", userInfo.Email)
	}
}
